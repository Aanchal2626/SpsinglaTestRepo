const { pool } = require("../helpers/database.helper.js");
const { v4: uuidv4 } = require("uuid");
const moment = require("moment");
const AWS = require("aws-sdk");
const path = require("path");

// AWS Config
AWS.config.update({
	accessKeyId: process.env.BUCKET_KEY,
	secretAccessKey: process.env.BUCKET_SECRET,
	region: process.env.BUCKET_REGION,
});

// Initializing S3
const s3 = new AWS.S3();

const documentController = {};

documentController.generateDocumentNumber = async (req, res) => {
	try {
		let inputs = req.query;
		let token = req.session.token;
		if (token.user_role == "3") {
			return res.send({ status: 0, msg: "Access Denie Insufficient Permissions." });
		}
		if (!inputs.site_id) {
			return res.send({ status: 0, msg: "Invalid Site Id" });
		}

		let dataFromDb = await pool.query(`SELECT * FROM sites WHERE site_id = ${inputs.folder_id}`);

		if (dataFromDb.rows.length == 0) {
			return res.send({ status: 0, msg: "Site record not found" });
		}
		res.send({ status: 1, msg: "Success", payload: dataFromDb.rows[0] });
	} catch (error) {
		console.error(error);
		res.send({ status: 0, msg: "Internal Server Error" });
	}
};

documentController.getDocumentReference = async (req, res) => {
	try {
		let inputs = req.query;
		let token = req.session.token;
		if (token.user_role == "3") {
			return res.send({ status: 0, msg: "Access Denie Insufficient Permissions." });
		}
		if (!inputs.query) {
			return res.send({ status: 0, msg: "Invalid Query" });
		}

		let referenceQuery = "SELECT doc_number FROM documents WHERE doc_number ILIKE $1";
		let dataFromDb = await pool.query(referenceQuery, [`%${inputs.query}%`]);
		let referenceNumbers = dataFromDb.rows.map((row) => row.doc_number);
		res.send({ status: 1, msg: "Success", payload: referenceNumbers });
	} catch (error) {
		console.error(error);
		res.send({ status: 0, msg: "Internal Server Error" });
	}
};

documentController.saveDraft = async (req, res) => {
	try {
		let inputs = req.body;
		let token = req.session.token;
		console.log(inputs);
		if (token.user_role == "3") {
			return res.send({ status: 0, msg: "Access Denied Insufficient Permissions." });
		}

		// getting site id
		let { rows: siteId } = await pool.query(`SELECT site_id FROM sites WHERE site_name = $1`, [inputs.doc_site]);

		// getting folder id
		let { rows: folderId } = await pool.query(`SELECT site_id FROM sites WHERE site_name = $1`, [inputs.doc_folder]);

		// UPDATING REFERNCES
		if (inputs.doc_reference) {
			let references = Array.isArray(inputs.doc_reference) ? inputs.doc_reference : [inputs.doc_reference];
			references = references.filter((reference) => reference.trim() !== "");

			if (references.length > 0) {
				// Delete existing references for the document
				const deleteQuery = `DELETE FROM doc_reference_junction WHERE doc_junc_number = $1`;
				await pool.query(deleteQuery, [inputs.doc_number]);

				// Insert new references with the current document number as the replied value
				const valuesString = references.map((_, i) => `($1, $${i + 2})`).join(", ");
				const referenceValues = [inputs.doc_number, ...references.map((ref) => ref.trim())];
				const refQuery = `INSERT INTO doc_reference_junction (doc_junc_number, doc_junc_replied) VALUES ${valuesString}`;
				await pool.query(refQuery, referenceValues);
			}
		}

		const generateInsertQuery = (data) => {
			delete data.doc_file;
			if (inputs.doc_reference && Array.isArray(inputs.doc_reference)) {
				inputs.doc_reference = inputs.doc_reference.join(",");
			}
			data["doc_uploaded_by"] = token.user_name;
			data["doc_uploaded_by_id"] = token.user_id;
			data["doc_uploaded_at"] = moment().format("MM/DD/YYYY");
			data["doc_status"] = "DRAFTED";

			const keys = Object.keys(data);
			const columns = keys.join(", ");
			const values = keys.map((_, i) => `$${i + 1}`).join(", ");
			const updateValues = keys
				.map((key, i) => `${key} = EXCLUDED.${key}`)
				.filter((key) => key !== "doc_number") // Exclude `doc_number` from update
				.join(", ");

			const query = `INSERT INTO documents (${columns}) VALUES (${values}) ON CONFLICT (doc_number) DO UPDATE SET ${updateValues};`;
			return { query, values: Object.values(data) };
		};

		const { query, values } = generateInsertQuery(inputs);
		await pool.query(query, values);
		let selectQuery = `SELECT COUNT(*) FROM documents WHERE doc_number = '${inputs.doc_number}'`;
		let selectResult = await pool.query(selectQuery);
		selectResult = selectResult?.rows[0]?.count;

		if (selectResult == 0 && inputs.doc_type == "OUTGOING") {
			let updateSiteRecordQuery = `
            UPDATE sites
            SET site_record_value = site_record_value + 1
            WHERE site_name = '${inputs.doc_folder}';
        `;
			await pool.query(updateSiteRecordQuery);
		}

		// ADDING HISTORY
		await pool.query(`INSERT INTO doc_history_junction (dhj_doc_number, dhj_history_type, dhj_timestamp,dhj_history_blame,dhj_history_blame_user) VALUES ($1,$2,$3,$4,$5)`, [inputs.doc_number, "DRAFTED", moment().format("MM/DD/YYYY HH:mm:ss"), token.user_id, token.user_name]);

		res.send({ status: 1, msg: "Success", payload: inputs.doc_number });
	} catch (error) {
		console.error(error);
		res.send({ status: 0, msg: "Internal Server Error" });
	}
};

documentController.editDocument = async (req, res) => {
	try {
		let inputs = req.body;
		let token = req.session.token;
		if (token.user_role === "3") {
			return res.send({ status: 0, msg: "Access Denied: Insufficient Permissions." });
		}

		// getting site id
		let { rows: siteId } = await pool.query(`SELECT site_id FROM sites WHERE site_name = $1`, [inputs.doc_site]);

		// getting folder id
		let { rows: folderId } = await pool.query(`SELECT site_id FROM sites WHERE site_name = $1`, [inputs.doc_folder]);

		// Store the new doc_number
		let newDocNumber = inputs.new_doc_number || inputs.doc_number;

		// Step 1: Insert the new record with the updated doc_number
		const generateInsertQuery = (data) => {
			delete data.doc_file;
			if (inputs.doc_reference && Array.isArray(inputs.doc_reference)) {
				inputs.doc_reference = inputs.doc_reference.join(",");
			}
			data["doc_uploaded_by"] = token.user_name;
			data["doc_uploaded_by_id"] = token.user_id;
			data["doc_uploaded_at"] = moment().format("MM/DD/YYYY");

			if (inputs.insertedFlag) {
				data["doc_status"] = "UPLOADED";
			}
			delete data.insertedFlag;

			const keys = Object.keys(data);
			const columns = keys.join(", ");
			const values = keys.map((_, i) => `$${i + 1}`).join(", ");
			const updateValues = keys.map((key) => `${key} = EXCLUDED.${key}`).join(", ");

			const query = `INSERT INTO documents (${columns}) VALUES (${values})
						   ON CONFLICT (doc_number) DO NOTHING;`;
			return { query, values: Object.values(data) };
		};

		const { query: insertQuery, values: insertValues } = generateInsertQuery({
			...inputs,
			doc_number: newDocNumber,
		});
		await pool.query(insertQuery, insertValues);

		// Step 2: Delete the old record with the old doc_number
		const deleteQuery = `DELETE FROM documents WHERE doc_number = $1`;
		await pool.query(deleteQuery, [inputs.doc_number]);

		// Step 3: Update references and history with the new doc_number
		if (inputs.doc_reference) {
			let references = Array.isArray(inputs.doc_reference) ? inputs.doc_reference : [inputs.doc_reference];
			references = references.filter((reference) => reference.trim() !== "");

			if (references.length > 0) {
				// Update doc_reference_junction with the new doc_number
				const deleteReferencesQuery = `DELETE FROM doc_reference_junction WHERE doc_junc_number = $1`;
				await pool.query(deleteReferencesQuery, [inputs.doc_number]);

				const valuesString = references.map((_, i) => `($1, $${i + 2})`).join(", ");
				const referenceValues = [newDocNumber, ...references.map((ref) => ref.trim())];
				const insertReferencesQuery = `INSERT INTO doc_reference_junction (doc_junc_number, doc_junc_replied) VALUES ${valuesString}`;
				await pool.query(insertReferencesQuery, referenceValues);
			}
		}

		// Update doc_history_junction with the new doc_number
		await pool.query(
			`INSERT INTO doc_history_junction (dhj_doc_number, dhj_history_type, dhj_timestamp, dhj_history_blame, dhj_history_blame_user) 
			VALUES ($1, $2, $3, $4, $5)`,
			[newDocNumber, "UPDATED", moment().format("MM/DD/YYYY HH:mm:ss"), token.user_id, token.user_name]
		);

		res.send({ status: 1, msg: "Success", payload: newDocNumber });
	} catch (error) {
		console.error(error);
		res.send({ status: 0, msg: "Internal Server Error" });
	}
};

documentController.createDocument = async (req, res) => {
	try {
		let inputs = req.body;
		let token = req.session.token;
		if (token.user_role == "3") {
			return res.send({ status: 0, msg: "Access Denied Insufficient Permissions." });
		}

		if (!req.file) {
			return res.send({ status: 0, msg: "No file uploaded" });
		}

		inputs.doc_number = inputs.doc_number.replace(/\s/g, "");

		if (inputs.doc_reference && Array.isArray(inputs.doc_reference)) {
			inputs.doc_reference = inputs.doc_reference.join(",");
		}

		const fileName = uuidv4();

		const s3Params = {
			Bucket: process.env.BUCKET_NAME,
			Key: `docs/${fileName}.pdf`,
			Body: req.file.buffer,
			ContentType: req.file.mimetype,
		};

		const s3Response = await s3.upload(s3Params).promise();
		const pdfLocation = s3Response.Location;

		const generateInsertQuery = (data) => {
			const keys = Object.keys(data);
			const nonEmptyKeys = keys.filter((key) => {
				const value = data[key];
				return value !== undefined && value !== null && (Array.isArray(value) ? value.length > 0 : typeof value === "string" ? value.trim() !== "" : true);
			});

			nonEmptyKeys.push("doc_uploaded_by", "doc_uploaded_at", "doc_status", "doc_pdf_link", "doc_uploaded_by_id", "doc_source");
			const currentDate = moment().format("MM/DD/YYYY");
			data["doc_uploaded_by"] = token.user_name;
			data["doc_uploaded_by_id"] = token.user_id;
			data["doc_uploaded_at"] = currentDate;
			data["doc_status"] = "UPLOADED";
			data["doc_pdf_link"] = pdfLocation;
			data["doc_source"] = "FORM";
			const columns = nonEmptyKeys.join(", ");
			const valuesPlaceholder = nonEmptyKeys.map((_, i) => `$${i + 1}`).join(", ");
			const values = nonEmptyKeys.map((key) => data[key]);
			const updateValues = nonEmptyKeys
				.map((key, i) => {
					if (key !== "id") {
						return `${key} = EXCLUDED.${key}`;
					}
					return null;
				})
				.filter((value) => value !== null)
				.join(", ");

			const query = `INSERT INTO documents (${columns}) VALUES (${valuesPlaceholder}) ON CONFLICT (doc_number) DO UPDATE SET ${updateValues};`;
			return { query, values };
		};

		const { query, values } = generateInsertQuery(inputs);

		// Check if the document already exists
		let selectQuery = `SELECT COUNT(*) FROM documents WHERE doc_number = $1`;
		let selectResult = await pool.query(selectQuery, [inputs.doc_number]);
		let count = selectResult?.rows[0]?.count;

		// Execute the insert/update query
		await pool.query(query, values);

		// Maintaining site record to auto-generate document numbers
		if (count == 0 && inputs.doc_type == "OUTGOING") {
			const updateSiteRecordQuery = `
                UPDATE sites
                SET site_record_value = site_record_value + 1
                WHERE site_name = $1
            `;
			await pool.query(updateSiteRecordQuery, [inputs.doc_folder]);
		}

		// UPDATING REFERNCES
		if (inputs.doc_reference) {
			let references = Array.isArray(inputs.doc_reference) ? inputs.doc_reference : [inputs.doc_reference];
			references = references.filter((reference) => reference.trim() !== "");

			if (references.length > 0) {
				// Delete existing references for the document
				const deleteQuery = `DELETE FROM doc_reference_junction WHERE doc_junc_number = $1`;
				await pool.query(deleteQuery, [inputs.doc_number]);

				// Insert new references with the current document number as the replied value
				const valuesString = references.map((_, i) => `($1, $${i + 2})`).join(", ");
				const referenceValues = [inputs.doc_number, ...references.map((ref) => ref.trim())];
				const refQuery = `INSERT INTO doc_reference_junction (doc_junc_number, doc_junc_replied) VALUES ${valuesString}`;
				await pool.query(refQuery, referenceValues);
			}
		}

		// ADDING HISTORY
		await pool.query(`INSERT INTO doc_history_junction (dhj_doc_number, dhj_history_type, dhj_timestamp,dhj_history_blame,dhj_history_blame_user) VALUES ($1,$2,$3,$4,$5)`, [inputs.doc_number, "UPDATED", moment().format("MM/DD/YYYY HH:mm:ss"), token.user_id, token.user_name]);

		res.send({ status: 1, msg: "Success", payload: inputs.doc_number });
	} catch (error) {
		res.send({ status: 0, msg: "Something Went Wrong" });
		console.error(error);
	}
};

documentController.getFilteredDocuments = async (req, res) => {
    try {
        let inputs = req.body;
        let token = req.session.token;
        const page = inputs.page || 1;
        const pageSize = inputs.limit || 10;
        const offset = (page - 1) * pageSize;
        let baseQuery = `
            SELECT d.*,string_agg(j.doc_junc_number, ', ') AS doc_replied_vide
            FROM documents d
            LEFT JOIN doc_reference_junction j ON j.doc_junc_replied = d.doc_number
        `;;
        let conditions = [];
        let joins = "";
        let folderQuery = ''
        // Handle folder permissions based on user role
        if (token.user_role == "0") {
            // Admin role
            folderQuery = `
          SELECT s.*, sp.site_name as site_parent_name
          FROM sites s
          LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
          WHERE s.site_parent_id != 0
          ORDER BY s.site_name`;
        } else {
            // Non-admin role with permissions
            folderQuery = `
          SELECT s.*, sp.site_name as site_parent_name
          FROM sites s
          JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
          LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
          WHERE usj.usj_user_id = ${token.user_id} AND s.site_parent_id != 0
          ORDER BY s.site_name`;
            let { rows: folderPermission } = await pool.query(folderQuery);
            const permission = folderPermission.map((fp) => `'${fp.site_name.replace(/'/g, "''")}'`).join(", ");
            if (permission.length > 0) {
                joins += ` JOIN sites s ON d.doc_folder = s.site_name`;
                conditions.push(`s.site_name IN (${permission})`);
            } else if (permission.length == 0 && token.user_role != "0") {
                return res.json({ status: 1, msg: "Success", payload: { documents: [], totalPages: 0, currentPage: page } });
            }
        }
        // Handle filters from inputs.activeFilter
        for (const [field, filter] of Object.entries(inputs.activeFilter)) {
            if (filter.type === "multiple") {
                const values = filter.value.map((val) => `'${val.replace(/'/g, "''")}'`).join(", ");
				values &&  conditions.push(`d.${field} IN (${values})`);
            } else if (filter.type === "text") {
				filter?.value &&  conditions.push(`LOWER(d.${field}) LIKE LOWER('%${filter.value.replace(/'/g, "''")}%')`);
            } else if (filter.type === "keyword") {
				baseQuery += `
				JOIN doc_metadata dm 
				ON d.doc_number = dm.dm_id 
				AND (
				 LOWER(dm.dm_id) LIKE LOWER('%${filter.value}%')
				  OR d.doc_storage_location = '${filter.value}'
				  OR d.doc_from = '${filter.value}'
				  OR d.doc_to = '${filter.value}'
				  OR LOWER(dm.dm_ocr_content) LIKE LOWER('%${filter.value}%')
				  )
			  `;
			  
            }
        }
		
		
        // Handle sorting
        let orderByClause = "";
        if (inputs.sort && Object.keys(inputs.sort).length > 0) {
            const sortFields = Object.entries(inputs.sort).map(([field, direction]) => {
                const dir = direction.toLowerCase() === "asc" ? "ASC" : "DESC";
                return `d.${field} ${dir}`;
            });
            orderByClause = `ORDER BY ${sortFields.join(", ")}`;
        } else {
            orderByClause = `ORDER BY d.doc_number DESC`;
        }
        // Build the WHERE clause
        if (conditions.length > 0) {
            baseQuery += " WHERE " + conditions.join(" AND ");
        }
        // Query to get the total count of documents
        let countQuery = `
        SELECT COUNT(DISTINCT d.doc_number) as total
        FROM documents d
        LEFT JOIN doc_reference_junction j ON j.doc_junc_replied = d.doc_number
		${conditions.length ?" WHERE " + conditions.join(" AND "):"" }
      `;
	  
        let { rows: countResult } = await pool.query(countQuery);
        const totalDocuments = countResult[0].total; 
        const totalPages = Math.ceil(totalDocuments / pageSize);		
        // Main query with pagination
        let query = `
        ${baseQuery}
        GROUP BY
          d.doc_id,
          d.doc_number,
          d.doc_site,
          d.doc_folder,
          d.doc_type,
          d.doc_status,
          d.doc_purpose,
          d.doc_subject,
          d.doc_from,
          d.doc_to,
          d.doc_storage_location,
          d.doc_reference,
          d.doc_created_at
        ${orderByClause}
        LIMIT ${pageSize}
        OFFSET ${offset}
      `;
	  
        // Execute the main query
        let { rows: documents } = await pool.query(query);
        res.json({
            status: 1,
            msg: "Success",
            payload: {
                documents,
                totalPages,
                currentPage: page,
            },
        });
    } catch (err) {
        console.error("Error fetching filtered documents:", err);
        res.json({ status: 0, msg: "Internal Server Error" });
    }
};

documentController.uploadAttachment = async (req, res) => {
	try {
		let inputs = req.body;
		if (!req.file) {
			return res.send({ status: 0, msg: "No file uploaded" });
		}

		const fileExtension = path.extname(req.file.originalname);
		const fileName = uuidv4();

		const s3Params = {
			Bucket: process.env.BUCKET_NAME,
			Key: `attachments/${fileName}${fileExtension}`,
			Body: req.file.buffer,
			ContentType: req.file.mimetype,
		};

		const s3Response = await s3.upload(s3Params).promise();
		const attachmentLocation = s3Response.Location;

		await pool.query(`INSERT INTO doc_attachment_junction (daj_doc_number, daj_attachment_name, daj_attachment_link, daj_attachment_upload_date) VALUES ($1, $2, $3, $4)`, [inputs.doc_number, inputs.doc_attachment, attachmentLocation, moment().format("MM/DD/YYYY")]);

		res.send({ status: 1, msg: "Attachment Uploaded" });
	} catch (err) {
		console.error("Error Uploading Attachments", err);
		res.json({ status: 0, msg: "Internal Server Error" });
	}
};

documentController.recordDocumentViewed = async (req, res) => {
	try {
		let inputs = req.body;
		let token = req.session.token;
		await pool.query(`INSERT INTO doc_history_junction (dhj_doc_number, dhj_history_type, dhj_timestamp,dhj_history_blame,dhj_history_blame_user) VALUES ($1,$2,$3,$4,$5)`, [inputs.doc_number, "VIEWED", moment().format("MM/DD/YYYY HH:mm:ss"), token.user_id, token.user_name]);
	} catch (err) {
		console.error("Error Uploading Attachments", err);
		res.json({ status: 0, msg: "Internal Server Error" });
	}
};

module.exports = documentController;
