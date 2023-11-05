const Mustache = require('mustache');
const fs = require('fs');

const filename = "template.html";
const local_path = "./";

function random(b, e) {
    return Math.round(Math.random() * (e - b) + b)
}

module.exports = async function(event, context = null) {
    let username = "hanfeiyu";
    let size = 1000;

    var random_numbers = new Array(size);
    for(var i = 0; i < size; ++i) {
        random_numbers[i] = random(0, 100)
    }

    var input = {
        cur_time: new Date().toLocaleString(),
        username: username,
        random_numbers: random_numbers
    }

    let result = new Promise((resolve, reject) => {
        fs.readFile(local_path + filename, "utf-8", (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve({"result": Mustache.render(data, input)});
            }
        })
    })
    await result;
    return result;
}

// var event = {
//     "username": "hanfeiyu",
//     "size": 1000
// }
// var res = require("./handler")(event, null)
// console.log(res)
