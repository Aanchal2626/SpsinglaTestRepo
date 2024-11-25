const { pool } = require("../helpers/database.helper.js");
const { v4: uuidv4 } = require("uuid");
const cron = require("node-cron");
const moment = require("moment");
const AWS = require("aws-sdk");
const textractHelper = require("../helpers/textract.helper.js");

// AWS Config
AWS.config.update({
	accessKeyId: process.env.BUCKET_KEY,
	secretAccessKey: process.env.BUCKET_SECRET,
	region: process.env.BUCKET_REGION,
});

// Initializing S3
const s3 = new AWS.S3();

const processDocument = async (cronJob) => {
	let cronId;

	try {
		// Check if any textract cron is already running
		const { rowCount: crons } = await pool.query(`SELECT * FROM crons WHERE cron_status = false AND cron_type = 'textract'`);

		if (crons) {
			console.log("Previous cron is still running");
			return;
		}

		// Fetch document to process
		let { rows: document } = await pool.query(`SELECT doc_number, doc_pdf_link FROM documents WHERE doc_ocr_status = false AND doc_pdf_link IS NOT NULL LIMIT 1`);

		if (document.length === 0) return;

		document = document[0];

		// Extract document details
		let doc_pdf_url = new URL(document.doc_pdf_link);
		let pdfName = doc_pdf_url.pathname.substring(1);

		// Generate cron_id
		cronId = uuidv4();

		// Insert into crons table with generated cron_id
		await pool.query("INSERT INTO crons (cron_id, cron_feed, cron_started_at, cron_type) VALUES ($1, $2, $3, $4)", [cronId, document.doc_number, getCurrentDateTime(), "textract"]);

		// Process Textract data
		const textractData = await textractHelper(process.env.BUCKET_NAME, pdfName, false);

		// Fetch document folder and site information
		const { rows: documentData } = await pool.query(`SELECT doc_folder, doc_site FROM documents WHERE doc_number = $1`, [document.doc_number]);
		const { doc_folder, doc_site } = documentData[0];

		// Update documents table
		const updateQuery = `
			UPDATE documents 
			SET 
				doc_ocr_status = true, 
				doc_ocr_pages = $1, 
				doc_ocr_content = $2
			WHERE doc_number = $3;
		`;

		const values = [textractData.totalPagesProcessed, textractData.textractResult, document.doc_number];

		const insertDocStatsQuery = `
			INSERT INTO doc_stats (doc_folder, doc_site, doc_total_pages, doc_total_doc) 
			VALUES ($1, $2, $3, 1) 
			ON CONFLICT (doc_folder) 
			DO UPDATE SET 
				doc_total_pages = doc_stats.doc_total_pages + EXCLUDED.doc_total_pages,
				doc_total_doc = doc_stats.doc_total_doc + 1;
		`;

		const docStatsValues = [doc_folder, doc_site, textractData.totalPagesProcessed];

		await pool.query(insertDocStatsQuery, docStatsValues);

		await pool.query(updateQuery, values);

		// Update crons table with cron_id
		await pool.query(`UPDATE crons SET cron_stopped_at = $1, cron_status = true WHERE cron_id = $2`, [getCurrentDateTime(), cronId]);

		console.log("Content Updated Successfully");
	} catch (err) {
		// Catch block updates for crons table
		const cronError = err.toString();
		await pool.query(`UPDATE crons SET cron_stopped_at = $1, cron_status = true, cron_flagged = true, cron_error = $2 WHERE cron_id = $3`, [getCurrentDateTime(), cronError, cronId]);
		console.error("CRON JOB ERROR", err);
	}
};

const getCurrentDateTime = () => {
	return moment().format("DD/MM/YYYY hh:mm:ss A");
};

const job = cron.schedule("*/30 * * * * *", () => processDocument(job));
