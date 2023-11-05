const filename = "exodia.png";
const local_path = "./";
const tesseract = require('tesseractocr')

function ocr(key) {
    text = new Promise((resolve, reject) => {
        let text = tesseract.recognize(Buffer.from(key, "base64"), (err, text) => {
	    if (err) {
                var response = {
                    statusCode: 500,
                    body: "Error!"
                };
	        resolve(response);
	    } else {
                var response = {
                    statusCode: 200,
                    body: text
                };
	        resolve(response);
	    }
	});
    });
        
    return text;
}

module.exports = function(event, context = null) {
    text = ocr(local_path + filename);
    return {"result": "ok"}
}

// var event = {}
// var res = require("./handler")(event, null)
// console.log(res)
