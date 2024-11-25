let router = require("express").Router();
let documentController = require("../controllers/document.controller");
const multer = require("multer");

const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

router.get("/generate-document-number", documentController.generateDocumentNumber);
router.get("/get-document-reference", documentController.getDocumentReference);
router.post("/save-draft", documentController.saveDraft);
router.post("/edit-document", documentController.editDocument);
router.post("/create-document", upload.single("doc_file"), documentController.createDocument);
router.post("/upload-attachment", upload.single("doc_file"), documentController.uploadAttachment);
router.post("/get-filtered-documents", documentController.getFilteredDocuments);
router.post("/record-document-viewed", documentController.recordDocumentViewed);

module.exports = router;
