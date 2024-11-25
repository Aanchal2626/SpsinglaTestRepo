let router = require("express").Router();
const authController = require("../controllers/auth.controller.js");

router.post("/login", authController.login);
router.get("/logout", authController.logout);
router.post("/reset-otp", authController.sendResetOTP);
router.post("/verify-otp", authController.verifyResetOTP);
router.post("/reset-password", authController.resetPassword);

module.exports = router;
