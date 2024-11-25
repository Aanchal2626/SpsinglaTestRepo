/* ॐ नमः शिवाय */
// Importing Packages
const express = require("express");
const dotenv = require("dotenv").config();
const path = require("path");
const redis = require("redis");
const connectRedis = require("connect-redis");
const session = require("express-session");
const cookieParser = require("cookie-parser");
const router = require("./app/routes/router.js");
const devEnvivronment = process.env.NODE_ENV;

//require("./app/crons/textract.cron.js");
//require("./app/crons/openai.cron.js");

// Creating Express Application
const app = express();

// Initializing Port
const port = process.env.PORT || 3000;

// Initializing Globals
global.app = app;
global.basePath = __dirname;

// Initializing Redis
let RedisStore, redisClient;
if (devEnvivronment === "development") {
	RedisStore = connectRedis(session);
	redisClient = redis.createClient({
		host: "http://localhost/",
		port: 6379,
		legacyMode: true,
	});
	redisClient.connect();

	redisClient.on("error", function (err) {
		console.log("Could not establish a connection with redis. " + err);
	});
	redisClient.on("connect", function (err) {
		console.log("Connected to redis successfully 👍👍👍👍👍👍👍👍");
	});
}

// Setting EJS
app.use(express.static(__dirname + ""));
app.set("views", [path.join(__dirname, "app/views/")]);
app.set("view engine", "ejs");

// Setting Session Middleware
app.use(
	session({
		secret: "verySecretKey",
		cookie: {
			secure: false,
			httpOnly: true,
			maxAge: 24 * 60 * 60 * 1000,
		},
		saveUninitialized: true,
		resave: false,
		...(process.env.NODE_ENV === "development" && {
			store: new RedisStore({ client: redisClient }),
		}),
	})
);

// Parsing Data
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true, limit: "50mb" }));
app.use(cookieParser());

// Allowing CORS
app.use(function (req, res, next) {
	res.setHeader("Access-Control-Allow-Origin", "*");
	next();
});

process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

// Listening on routes
app.use("/", router);

// Starting server on port
app.listen(port, function () {
	console.log("Server listening on port....", port);
});
