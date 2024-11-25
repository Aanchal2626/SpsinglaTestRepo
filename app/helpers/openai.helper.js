const OpenAIApi = require("openai");

const openai = new OpenAIApi({ apiKey: process.env.OPENAI_API_KEY });

const processOpenAI = async (text) => {
	const prompt = `
    Extract and structure the following information from the given text in JSON object:
    1. Letter Number
    2. References
    3. Date
    4. Subject

    Return a JSON object with these keys:
    {
    "letter_number": "",
    "references": "",
    "date": "",
    "subject": ""
    }

    Rules and Clarifications :- 
    1. letter_number: Include only the first exact alphanumeric code in the specified format found in the document header. Ignore any prefixes like "Ref:" or "No:". Example: SPSCPL/BSRDCL/GANGAPATH/22-23/109.
    2. references : Include only valid reference numbers found after the Letter Number. Exclude any labels or prefixes. Multiple references should be comma-separated without spaces. Example: SPSCPL/BSRDCL/GANGAPATH/22-23/110,AE/TRUMPET/TL/EPC/2023/001
    3. date : Strictly follow 'dd/mm/yyyy' format. Convert any other date formats encountered to this standard format.
    4. subject : Include the whole subject section usually starts with Subject: , Sub: , Sub-

    Text: ${text}`;

	try {
		const response = await openai.chat.completions.create({
			messages: [{ role: "user", content: prompt }],
			model: "ft:gpt-3.5-turbo-1106:personal:002:9qxghOlU",
			temperature: 0.2,
			response_format: { type: "json_object" },
		});
		return response;
	} catch (error) {
		console.error("Error extracting information from OpenAI:", error);
		return null;
	}
};

module.exports = processOpenAI;
