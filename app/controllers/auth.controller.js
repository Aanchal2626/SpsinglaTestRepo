const { pool } = require("../helpers/database.helper.js");
const moment = require("moment");
const nodemailer = require("nodemailer");
const authController = {};

authController.login = async (req, res) => {
	try {
		let inputs = req.body;

		if (!inputs.user_email) {
			return res.send({ status: 0, msg: "Invalid Email" });
		}

		if (!inputs.user_password) {
			return res.send({ status: 0, msg: "Invalid Password" });
		}

		let result = await pool.query(`SELECT * FROM users WHERE user_email = $1`, [inputs.user_email]);

		if (result.rows.length == 0) {
			return res.send({ status: 0, msg: "User not found" });
		}

		let user = result.rows[0];

		if (!user.user_status) {
			return res.send({ status: 0, msg: "User Suspended" });
		}

		if (inputs.user_password != user.user_password) {
			return res.send({ status: 0, msg: "Incorrect Password" });
		}

		delete user.user_password;

		req.session.token = user;

		res.send({ status: 1, msg: "Login successful" });
	} catch (err) {
		res.send({ status: 0, msg: "Something Went Wrong" });
		console.error(err);
	}
};

authController.sendResetOTP = async (req, res) => {
	try {
		let inputs = req.body;

		if (!inputs.user_email) {
			return res.send({ status: 0, msg: "Invalid Email" });
		}

		let result = await pool.query(`SELECT * FROM users WHERE user_email = $1`, [inputs.user_email]);

		if (result.rows.length == 0) {
			return res.send({ status: 0, msg: "User not found" });
		}

		let user = result.rows[0];

		if (!user.user_status) {
			return res.send({ status: 0, msg: "User Suspended" });
		}

		let currentTime = moment().unix();
		if (user.user_otp_expire && currentTime < user.user_otp_expire) {
			return res.send({ status: 1, msg: "OTP Still Valid" });
		}

		let otpExpireTimestamp = moment().add(5, "minutes").unix();
		let otp = Math.floor(100000 + Math.random() * 900000);

		const transporter = nodemailer.createTransport({
			host: process.env.SMTP_HOST,
			port: process.env.SMTP_PORT,
			secure: true,
			auth: {
				user: process.env.SMTP_USER,
				pass: process.env.SMTP_PASSWORD,
			},
		});

		const mailOptions = {
			from: process.env.SMTP_USER,
			to: user.user_email,
			subject: "SPS DOCS CONTROLLER PASSWORD RESET",
			text: `Your password reset OTP is: ${otp}`,
		};

		transporter.sendMail(mailOptions, (error, info) => {
			if (error) {
				console.log("Error sending email: " + error);
			} else {
				console.log("Email sent: " + info.response);
			}
		});

		await pool.query(`UPDATE users SET user_otp = $1, user_otp_expire = $2 WHERE user_id = $3`, [otp, otpExpireTimestamp, user.user_id]);

		res.send({ status: 1, msg: "Sent OTP to Email" });
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

authController.verifyResetOTP = async (req, res) => {
	try {
		let inputs = req.body;
		if (!inputs.user_email) return res.send({ status: 0, msg: "Invalid Email" });
		if (!inputs.user_otp) return res.send({ status: 0, msg: "Invalid OTP" });
		let result = await pool.query(`SELECT * FROM users WHERE user_email = $1`, [inputs.user_email]);
		if (result.rows.length == 0) return res.send({ status: 0, msg: "User not found" });
		let user = result.rows[0];

		if (user.user_otp != inputs.user_otp) {
			return res.send({ status: 0, msg: "Incorrect OTP" });
		}
		let now = moment();
		let otpExpireTime = moment.unix(user.user_otp_expire);
		if (now.isAfter(otpExpireTime)) return res.send({ status: 0, msg: "OTP Expired" });
		if (user.user_otp == inputs.user_otp) return res.send({ status: 1, msg: "Success" });
	} catch (err) {
		res.send({ status: 0, msg: "Something Went Wrong" });
		console.error(err);
	}
};

authController.resetPassword = async (req, res) => {
	try {
		let inputs = req.body;
		if (!inputs.user_email) return res.send({ status: 0, msg: "Invalid Email" });
		if (!inputs.newPassword) return res.send({ status: 0, msg: "Invalid Password" });
		await pool.query(`UPDATE users SET user_password = $1 , user_otp = null, user_otp_expire = null WHERE user_email = $2`, [inputs.newPassword, inputs.user_email]);
		res.send({ status: 1, msg: "Success" });
	} catch (err) {
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

authController.addZimbraPassword = async (req, res) => {
	try {
		let inputs = req.body;
		let token = req.session.token;
		if (!inputs.user_email_password) return res.send({ status: 0, msg: "Invalid Password" });
		let query = `UPDATE users SET user_email_password = $1 WHERE user_id = $2`;
		let users = await pool.query(query, [inputs.user_email_password, token.user_id]);
		if (users) {
			req.session.token.user_email_password = inputs.user_email_password;
			res.send({ status: 1, msg: "Authentication Success" });
		} else {
			return res.send({ status: 0, msg: "Something Went Wrong" });
		}
	} catch (err) {
		console.log(err);
		res.send({ status: 0, msg: "Something Went Wrong" });
	}
};

authController.logout = async (req, res) => {
	req.session.destroy();
	res.redirect("/");
};

module.exports = authController;
