const fs = require('fs');
const https = require('follow-redirects').https;

const filename = "hpx.zip";
const url = "https://github.com/STEllAR-GROUP/hpx/archive/1.4.0.zip";
const local_path = "./";

function streamToPromise(stream) {
    return new Promise((resolve, reject) => {
        stream.on("close", () => {
        resolve()
        });
        stream.on("error", reject);
    })
}
  
module.exports = async function(event, context = null) {
    let couch_link = "http://whisk_admin:some_passw0rd@172.17.0.1:5984";
    let db_name = "ul";

    // Connect to couchdb
    const nano = require('nano')(couch_link);
    try {
        nano.use(db_name);
    } catch (e) {
        await nano.db.create(db_name);
        const database = nano.use(db_name);
        await database.insert({"success": true}, 'file');
    }
    const database = nano.use(db_name);
    var doc = await database.get('file');
    // console.log(doc)

    // Download file
    var file = fs.createWriteStream(local_path + filename)
    https.get(url, (res) => {
        res.pipe(file)
    })
    let upload = streamToPromise(file).then(async () => {
        var data = fs.readFileSync(local_path + filename, "utf-8")
        try {
            await database.attachment.insert(doc._id, filename, data, "application/zip", {'rev': doc._rev})
        } catch (e) {
            console.log(e);
        }
    })
    await upload;
    return {"result": doc};
}
  
// var event = {
//     "couch_link": "http://whisk_admin:some_passw0rd@172.17.0.1:5984",
//     "db_name": "ul"
// }
// var res = require("./handler")(event, null)
// console.log(res)
