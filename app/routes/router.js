let router = require("express").Router();
let renderRouter = require("./render.router.js");
let authRouter = require("./auth.router.js");
let documentRouter = require("./document.router.js");
let usersRouter = require("./users.router.js");
let sitesRouter = require("./sites.router.js");

router.use("/", renderRouter);
router.use("/auth", authRouter);
router.use("/docs", documentRouter);
router.use("/users", usersRouter);
router.use("/sites", sitesRouter);

router.use((req, res) => {
	res.render("404.ejs");
});

module.exports = router;
