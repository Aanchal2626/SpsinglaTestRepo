let router = require("express").Router();
const sitesController = require("../controllers/sites.controller");

router.post("/save-site", sitesController.saveSite);
router.post("/save-folder", sitesController.saveFolder);

module.exports = router;
