const { pool } = require("../helpers/database.helper.js");
const textractHelper = require("../helpers/textract.helper.js");
const openAIHelper = require("../helpers/openai.helper.js");
const { v4: uuidv4 } = require("uuid");
const cron = require("node-cron");
const moment = require("moment");
const AWS = require("aws-sdk");

// AWS Config
AWS.config.update({
	accessKeyId: process.env.BUCKET_KEY,
	secretAccessKey: process.env.BUCKET_SECRET,
	region: process.env.BUCKET_REGION,
});

const s3 = new AWS.S3();

const processDocument = async (cronJob) => {
	let cronId = uuidv4();
	let tempLetterNumber = false;
	try {
		const { rowCount: crons } = await pool.query(`SELECT * FROM crons WHERE cron_status = false AND cron_type = 'open_ai' `);

		if (crons) return;

		const params = {
			Bucket: process.env.BUCKET_NAME,
			Prefix: "unzipped_uploads/",
		};
		const s3Data = await s3.listObjectsV2(params).promise();
		const pdfFiles = s3Data.Contents.filter((file) => file.Key.endsWith(".pdf"));

		if (pdfFiles.length === 0) {
			return;
		}

		await pool.query(`UPDATE misc SET misc_run_count = misc_run_count + 1 WHERE misc_id = 1;`);

		const pdfFileKey = pdfFiles[0].Key;

		await pool.query("INSERT INTO crons (cron_id, cron_feed, cron_type, cron_started_at) VALUES ($1, $2, $3, $4)", [cronId, pdfFileKey, "open_ai", getCurrentDateTime()]);

		let textractData = await textractHelper(process.env.BUCKET_NAME, pdfFileKey, true);
		let extractData = await openAIHelper(textractData.textractResult);
		extractedOpenAIData = JSON.parse(extractData.choices[0].message.content);
		if (!extractedOpenAIData) {
			await pool.query(`UPDATE crons SET cron_error = $1, cron_status = $2, cron_flagged = $3, cron_stopped_at = $4 WHERE cron_id = $5`, ["Emptyy AI Response", true, true, getCurrentDateTime(), cronId]);

			await s3
				.copyObject({
					Bucket: process.env.BUCKET_NAME,
					CopySource: `${process.env.BUCKET_NAME}/${pdfFileKey}`,
					Key: `flagged_uploads/${pdfFileKey.split("/").pop()}`,
				})
				.promise();

			await s3
				.deleteObject({
					Bucket: process.env.BUCKET_NAME,
					Key: pdfFileKey,
				})
				.promise();

			return;
		} else if (!extractedOpenAIData.letter_number) {
			extractedOpenAIData.letter_number = uuidv4();
			tempLetterNumber = true;
		}

		await pool.query(`UPDATE crons SET cron_response = $1 WHERE cron_feed = $2`, [JSON.stringify(extractedOpenAIData), pdfFileKey]);

		let siteCode = (pdfFileKey.replace("unzipped_uploads/", "").match(/^(\d+)_/) || [])[1];
		let uploadedById = (pdfFileKey.replace("unzipped_uploads/", "").match(/^\d+_(\d+)_/) || [])[1];
		let { rows: userDataFromDb } = await pool.query(`SELECT user_name FROM users WHERE user_id = $1`, [uploadedById]);
		let { rows: siteDataFromDb } = await pool.query(`SELECT * FROM sites WHERE site_id = $1`, [siteCode]);
		let { rows: parentSiteFromDb } = await pool.query(`SELECT * FROM sites WHERE site_id = ${siteDataFromDb[0].site_parent_id}`);

		const newFileName = `${uuidv4()}.pdf`;
		const newFileKey = `docs/${newFileName}`;

		await s3
			.copyObject({
				Bucket: process.env.BUCKET_NAME,
				CopySource: `${process.env.BUCKET_NAME}/${pdfFileKey}`,
				Key: newFileKey,
				MetadataDirective: "REPLACE",
				ContentDisposition: "inline",
				ContentType: "application/pdf",
			})
			.promise();

		await s3
			.deleteObject({
				Bucket: process.env.BUCKET_NAME,
				Key: pdfFileKey,
			})
			.promise();

		const fileUrl = `https://${process.env.BUCKET_NAME}.s3.${process.env.BUCKET_REGION}.amazonaws.com/${newFileKey}`;

		let document = {};

		document.doc_number = extractedOpenAIData.letter_number;
		if (!tempLetterNumber) document.doc_type = extractedOpenAIData.letter_number.includes("SPS/") ? "OUTGOING" : "INCOMING";
		document.doc_reference = extractedOpenAIData.references.replace(/\s+/g, "");
		document.doc_created_at = extractedOpenAIData.date;
		document.doc_subject = extractedOpenAIData.subject;
		document.doc_source = "BULK";
		document.doc_uploaded_at = moment().format("MM/DD/YYYY");
		document.doc_status = "INSERTED";
		document.doc_site = parentSiteFromDb[0].site_name;
		document.doc_folder = siteDataFromDb[0].site_name;
		document.doc_uploaded_by_id = uploadedById;
		document.doc_uploaded_by = userDataFromDb[0].user_name;
		document.doc_pdf_link = fileUrl;
		document.doc_ocr_status = false;

		await pool.query(
			`INSERT INTO documents (
				doc_number, doc_type, doc_reference, doc_created_at, doc_subject, doc_source, doc_uploaded_at, doc_status, doc_site, doc_folder, doc_uploaded_by_id, doc_uploaded_by, doc_pdf_link, doc_ocr_status
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
			ON CONFLICT (doc_number) 
			DO UPDATE SET 
				doc_type = EXCLUDED.doc_type,
				doc_reference = EXCLUDED.doc_reference,
				doc_created_at = EXCLUDED.doc_created_at,
				doc_subject = EXCLUDED.doc_subject,
				doc_source = EXCLUDED.doc_source,
				doc_uploaded_at = EXCLUDED.doc_uploaded_at,
				doc_status = EXCLUDED.doc_status,
				doc_site = EXCLUDED.doc_site,
				doc_folder = EXCLUDED.doc_folder,
				doc_uploaded_by_id = EXCLUDED.doc_uploaded_by_id,
				doc_uploaded_by = EXCLUDED.doc_uploaded_by,
				doc_pdf_link = EXCLUDED.doc_pdf_link,
				doc_ocr_status = EXCLUDED.doc_ocr_status;`,
			[document.doc_number, document.doc_type, document.doc_reference, document.doc_created_at, document.doc_subject, document.doc_source, document.doc_uploaded_at, document.doc_status, document.doc_site, document.doc_folder, document.doc_uploaded_by_id, document.doc_uploaded_by, document.doc_pdf_link, document.doc_ocr_status]
		);

		const { rows: documentData } = await pool.query(`SELECT doc_folder, doc_site FROM documents WHERE doc_number = $1;`, [document.doc_number]);

		const { doc_folder, doc_site } = documentData[0];

		await pool.query(
			`
			INSERT INTO doc_stats (doc_folder, doc_site, doc_total_pages, doc_total_doc) 
			VALUES ($1, $2, $3, 1) 
			ON CONFLICT (doc_folder) 
			DO UPDATE SET 
			  doc_total_pages = doc_stats.doc_total_pages + EXCLUDED.doc_total_pages,
			  doc_total_doc = doc_stats.doc_total_doc + 1;
			`,
			[doc_folder, doc_site, textractData.totalPagesProcessed]
		);

		// ADDING HISTORY
		await pool.query(`INSERT INTO doc_history_junction (dhj_doc_number, dhj_history_type, dhj_timestamp,dhj_history_blame,dhj_history_blame_user) VALUES ($1,$2,$3,$4,$5)`, [extractedOpenAIData.letter_number, "INSERTED", moment().format("MM/DD/YYYY HH:mm:ss"), uploadedById, userDataFromDb[0].user_name]);
		await pool.query(`UPDATE crons SET cron_status = $1, cron_stopped_at = $2 WHERE cron_feed = $3`, [true, getCurrentDateTime(), pdfFileKey]);
		console.log("Document processed and inserted successfully");
	} catch (error) {
		console.error(error);
		await pool.query(`UPDATE crons SET cron_error = $1, cron_status = $2, cron_flagged = $3, cron_stopped_at = $4 WHERE cron_feed = $5`, [error.message, true, true, getCurrentDateTime(), error.message]);
	}
};

const getCurrentDateTime = () => {
	const timeStamp = moment().format("DD/MM/YYYY hh:mm:ss A");
	return timeStamp;
};

const job = cron.schedule("*/30 * * * * *", () => processDocument(job));
