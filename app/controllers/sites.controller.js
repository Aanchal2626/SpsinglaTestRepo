const { pool } = require("../helpers/database.helper.js");
let userController = {};

userController.saveSite = async (req, res) => {
	try {
		const inputs = req.body;

		if (!inputs.site_name || inputs.site_name == "") {
			return res.send({ status: 0, msg: "Invalid Site Name" });
		}
		if (!inputs.site_code || inputs.site_code == "") {
			return res.send({ status: 0, msg: "Invalid Site Code" });
		}

		let site = await pool.query(`SELECT * FROM sites WHERE site_code = '${inputs.site_code}'`);
		site = site.rows[0];

		const query = `
        INSERT INTO sites (site_name, site_parent_id, site_code)
        VALUES ($1, $2, $3)
        ON CONFLICT (site_code)
        DO UPDATE SET
          site_name = EXCLUDED.site_name
        RETURNING *;
      `;

		let updatedSite = await pool.query(query, [inputs.site_name, 0, inputs.site_code]);
		updatedSite = updatedSite?.rows[0];
		if (updatedSite) {
			if (!site) {
				let insertSites = `
					INSERT INTO sites (site_name, site_parent_id, site_code, site_prefix) 
					VALUES 
					('${updatedSite.site_name} - HO', ${updatedSite.site_id}, '${updatedSite.site_code}1', 'SPS/HO/${updatedSite.site_code}/'),
					('${updatedSite.site_name} - CLIENT', ${updatedSite.site_id}, '${updatedSite.site_code}2', 'SPS/CLIENT/${updatedSite.site_code}/'),
					('${updatedSite.site_name} - SITE', ${updatedSite.site_id}, '${updatedSite.site_code}3', 'SPS/SITE/${updatedSite.site_code}/'),
					('${updatedSite.site_name} - AE', ${updatedSite.site_id}, '${updatedSite.site_code}4', 'SPS/AE/${updatedSite.site_code}/'),
					('${updatedSite.site_name} - EMAIL', ${updatedSite.site_id}, '${updatedSite.site_code}5', 'SPS/EMAIL/${updatedSite.site_code}/');
				`;
				await pool.query(insertSites);
			}
			res.send({ status: 1, msg: "Site saved successfully" });
		} else {
			res.send({ status: 0, msg: "Something went wrong" });
		}
	} catch (error) {
		console.error("Error saving site:", error);
		res.send({ status: 0, msg: "Something went wrong" });
	}
};

userController.saveFolder = async (req, res) => {
	try {
		const inputs = req.body;

		if (!inputs.folder_name || inputs.folder_name == "") {
			return res.send({ status: 0, msg: "Invalid Folder Name" });
		}
		if (!inputs.folder_code || inputs.folder_code == "") {
			return res.send({ status: 0, msg: "Invalid Folder Code" });
		}
		if (!inputs.folder_prefix || inputs.folder_prefix == "") {
			return res.send({ status: 0, msg: "Invalid Folder Prefix" });
		}
		inputs.folder_name = inputs.folder_site_name + " - " + inputs.folder_name;
		inputs.folder_prefix = inputs.folder_prefix.join(",");
		const query = `
        INSERT INTO sites (site_name, site_parent_id, site_code, site_prefix, site_record_value)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (site_code)
        DO UPDATE SET
          site_name = EXCLUDED.site_name,
          site_prefix = EXCLUDED.site_prefix,
          site_record_value = EXCLUDED.site_record_value
        RETURNING *;`;

		let updatedSite = await pool.query(query, [inputs.folder_name, inputs.folder_site, inputs.folder_code, inputs.folder_prefix, inputs.folder_record_value]);
		updatedSite = updatedSite?.rows[0];

		if (updatedSite) {
			// Adding Site Permissions
			let parentSiteId = await pool.query("SELECT site_parent_id FROM sites WHERE site_id = $1", [updatedSite.site_id]);
			parentSiteId = parentSiteId.rows[0].site_parent_id;

			// Removing Previous Permissions
			await pool.query("DELETE FROM users_sites_junction WHERE usj_site_id = $1;", [updatedSite.site_id]);
			await pool.query("DELETE FROM users_sites_junction WHERE usj_site_id = $1;", [parentSiteId]);

			for (let userId of inputs.user_permissions) {
				await pool.query("INSERT INTO users_sites_junction (usj_user_id, usj_site_id) VALUES ($1, $2)", [userId, parentSiteId]);
			}

			// Adding Folder Permissions
			for (let userId of inputs.user_permissions) {
				await pool.query("INSERT INTO users_sites_junction (usj_user_id, usj_site_id) VALUES ($1, $2)", [userId, updatedSite.site_id]);
			}
			res.send({ status: 1, msg: "Site saved successfully" });
		} else {
			res.send({ status: 0, msg: "Something went wrong" });
		}
	} catch (error) {
		console.error("Error saving site:", error);
		res.send({ status: 0, msg: "Something went wrong" });
	}
};

module.exports = userController;
