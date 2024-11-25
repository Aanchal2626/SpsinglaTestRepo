let router = require("express").Router();
const userController = require("../controllers/users.controller");

router.post("/save-user", userController.saveUser);

module.exports = router;
