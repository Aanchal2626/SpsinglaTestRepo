const { pool } = require("../helpers/database.helper.js");

const renderController = {};

renderController.renderNotFound = async (req, res) => {
	try {
		res.render("404.ejs");
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};
renderController.renderForgotPassword = async (req, res) => {
	try {
		res.render("forget-password.ejs");
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

renderController.renderDashboard = async (req, res) => {
	try {
		let token = req.session.token;

		let siteQuery;
		if (token.user_role === "0") {
			siteQuery = `SELECT * FROM sites WHERE site_parent_id = 0`;
		} else {
			siteQuery = `
                SELECT s.*
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                WHERE usj.usj_user_id = ${token.user_id} AND site_parent_id = 0
            `;
		}

		let { rows: sites } = await pool.query(siteQuery);
		res.render("dashboard.ejs", { token, sites });
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

renderController.renderDocuments = async (req, res) => {
	try {
		let token = req.session.token;
		let siteQuery, folderQuery;
		if (token.user_role === "0") {
			siteQuery = `SELECT * FROM sites WHERE site_parent_id = 0`;
			folderQuery = `
                SELECT s.*, sp.site_name as site_parent_name
                FROM sites s
                LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
                WHERE s.site_parent_id != 0
                ORDER BY s.site_name`;
		} else {
			siteQuery = `
                SELECT s.*
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                WHERE usj.usj_user_id = ${token.user_id} AND site_parent_id = 0
            `;
			folderQuery = `
                SELECT s.*, sp.site_name as site_parent_name
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
                WHERE usj.usj_user_id = ${token.user_id} AND s.site_parent_id != 0
                ORDER BY s.site_name
            `;
		}

		let siteFromDb = await pool.query(siteQuery);
		let folderFromDb = await pool.query(folderQuery);
		let sites = siteFromDb.rows;
		let folders = folderFromDb.rows;
		res.render("documents.ejs", { token, sites, folders });
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

renderController.renderBulkImport = async (req, res) => {
	try {
		let token = req.session.token;
		if (token.user_role === "0") {
			siteQuery = `SELECT * FROM sites WHERE site_parent_id = 0`;
			folderQuery = `
				SELECT s.*, sp.site_name as site_parent_name
				FROM sites s
				LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
				WHERE s.site_parent_id != 0
				ORDER BY s.site_name`;
		} else {
			siteQuery = `
				SELECT s.*
				FROM sites s
				JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
				WHERE usj.usj_user_id = ${token.user_id} AND site_parent_id = 0
			`;
			folderQuery = `
				SELECT s.*, sp.site_name as site_parent_name
				FROM sites s
				JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
				LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
				WHERE usj.usj_user_id = ${token.user_id} AND s.site_parent_id != 0
				ORDER BY s.site_name
			`;
		}
		let siteFromDb = await pool.query(siteQuery);
		let folderFromDb = await pool.query(folderQuery);
		let sites = siteFromDb.rows;
		let folders = folderFromDb.rows;

		let config = {
			keyId: process.env.BUCKET_KEY,
			accessKey: process.env.BUCKET_SECRET,
			region: process.env.BUCKET_REGION,
			bucketName: process.env.BUCKET_NAME,
		};
		res.render("bulk-import.ejs", { token, sites, folders, config });
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};
renderController.renderSingleDocument = async (req, res) => {
	let token = req.session.token;
	try {
		let siteQuery, folderQuery, documentQuery, referencesQuery;
		let doc_number = Buffer.from(req.params.id, "base64").toString("utf-8");

		// Query to get document details
		documentQuery = `
			 SELECT d.*, string_agg(j.doc_junc_number, ', ') AS doc_replied_vide
            FROM documents d
            LEFT JOIN doc_reference_junction j ON j.doc_junc_replied = d.doc_number
			WHERE d.doc_number = '${doc_number}'
			GROUP BY d.doc_id,d.doc_number
		`;

		let documentData = await pool.query(documentQuery);

		documentData = documentData?.rows[0];
		if (!documentData) return res.render("404");

		if (token.user_role != 0) {
			let permissionQuery = `
                SELECT s.*
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                WHERE usj.usj_user_id = ${token.user_id}
            `;
			let permittedSites = await pool.query(permissionQuery);
			permittedSites = permittedSites.rows;
			const isFolderPermitted = permittedSites.some((site) => site.site_name === documentData.doc_folder);
			if (!isFolderPermitted) {
				return res.render("404");
			}
		}

		if (token.user_role === "0") {
			siteQuery = `SELECT * FROM sites WHERE site_parent_id = 0`;
			folderQuery = `
                SELECT s.*, sp.site_name as site_parent_name
                FROM sites s
                LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
                WHERE s.site_parent_id != 0
                ORDER BY s.site_name
            `;
		} else {
			siteQuery = `
                SELECT s.*
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                WHERE usj.usj_user_id = ${token.user_id} AND site_parent_id = 0
            `;
			folderQuery = `
                SELECT s.*, sp.site_name as site_parent_name
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
                WHERE usj.usj_user_id = ${token.user_id} AND s.site_parent_id != 0
                ORDER BY s.site_name
            `;
		}

		let siteFromDb = await pool.query(siteQuery);
		let folderFromDb = await pool.query(folderQuery);
		let sites = siteFromDb.rows;
		let folders = folderFromDb.rows;

		if (documentData.doc_status == "DRAFTED") {
			res.render("create-document.ejs", { token, sites, folders, documentData });
		} else if (documentData.doc_status == "INSERTED") {
			res.render("edit-document.ejs", { token, sites, folders, documentData });
		} else {
			let { rows: docHistory } = await pool.query(`SELECT * FROM doc_history_junction WHERE dhj_doc_number = $1`, [documentData.doc_number]);
			let { rows: docAttachments } = await pool.query(`SELECT * FROM doc_attachment_junction WHERE daj_doc_number = $1`, [documentData.doc_number]);

			res.render("view-document.ejs", { token, sites, folders, documentData, docHistory, docAttachments });
		}
	} catch (error) {
		console.error(error);
		res.send("Internal Server Error");
	}
};

renderController.renderUsers = async (req, res) => {
	let token = req.session.token;
	try {
		let query = `SELECT * FROM users WHERE user_role != 0`;
		let users = await pool.query(query);
		users = users.rows;
		res.render("users.ejs", { token, users });
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

renderController.renderSites = async (req, res) => {
	let token = req.session.token;
	try {
		let query = `SELECT * FROM sites ORDER BY site_id ASC;`;
		let permissionsQuery = `
        SELECT users_sites_junction.*, users.user_name
        FROM users_sites_junction
        LEFT JOIN users ON users_sites_junction.usj_user_id = users.user_id;`;
		let usersQuery = `SELECT user_id,user_name FROM users WHERE user_role != 0`;
		let { rows: sites } = await pool.query(query);
		let { rows: permissions } = await pool.query(permissionsQuery);
		let { rows: users } = await pool.query(usersQuery);
		res.render("sites.ejs", { token, sites, permissions, users });
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

renderController.renderCreateDocument = async (req, res) => {
	let token = req.session.token;

	try {
		let siteQuery, folderQuery;
		if (token.user_role === "0") {
			siteQuery = `SELECT * FROM sites WHERE site_parent_id = 0`;
			folderQuery = `
                SELECT s.*, sp.site_name as site_parent_name
                FROM sites s
                LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
                WHERE s.site_parent_id != 0
                ORDER BY s.site_name`;
		} else {
			siteQuery = `
                SELECT s.*
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                WHERE usj.usj_user_id = ${token.user_id} AND site_parent_id = 0
            `;
			folderQuery = `
                SELECT s.*, sp.site_name as site_parent_name
                FROM sites s
                JOIN users_sites_junction usj ON s.site_id = usj.usj_site_id
                LEFT JOIN sites sp ON s.site_parent_id = sp.site_id
                WHERE usj.usj_user_id = ${token.user_id} AND s.site_parent_id != 0
                ORDER BY s.site_name
            `;
		}

		let siteFromDb = await pool.query(siteQuery);
		let folderFromDb = await pool.query(folderQuery);
		let sites = siteFromDb.rows;
		let folders = folderFromDb.rows;
		let documentData = [];
		res.render("create-document.ejs", { token, sites, folders, documentData });
	} catch (error) {
		console.error(error);
		res.status(500).send("Internal Server Error");
	}
};

module.exports = renderController;
