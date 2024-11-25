const authMiddleware = {};

authMiddleware.checkLoginStatus = async (req, res, next) => {
	if (req.session.token) {
		next();
	} else {
		res.render("login.ejs");
	}
};

module.exports = authMiddleware;
