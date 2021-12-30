const fs = require('fs');
const path = require('path');
const { parseStream } = require('@fast-csv/parse');
const { format } = require('@fast-csv/format');
const randomstring = require("randomstring");


let args = process.argv.slice(2);
if(args && args.length !=1)
{
    console.log("Please provide the input filename");
    return;
}

const csvInputFileName = args[0];
const inputStream = fs.createReadStream(path.resolve(__dirname, '.', 'input', csvInputFileName));
let newRows = [];
let newRowCount = 0;

parseStream(inputStream, { headers: true , skipRows: 0, ignoreEmpty: true})
    .on("data", function (row) {
        inputStream.pause();
        // do some heavy work
        for (var key of Object.keys(row)) {
            //console.log(key + " -> " + row[key] +  " -> "+ row[key].length);
            if(key && row["ID"].length>0 && key.toUpperCase()!=="ID" && row[key]!="")
                row[key] = randomstring.generate(15); //random string set to 15 characters
        }
        newRows.push(row);
        // when done resume the inputStream
        inputStream.resume();
    })
    .on("end", function (rowCount) {
        newRowCount = rowCount;
        console.log(`Parsed ${rowCount} rows`);

        const csvOutputFileName = `${csvInputFileName.split(".")[0]}-output.csv`;
        const csvOutputFile = fs.createWriteStream(path.resolve(path.resolve(__dirname, '.', 'output', csvOutputFileName)));
        const outputStream = format({ headers:true });

        outputStream.pipe(csvOutputFile);
        for(i=0; i<newRowCount; i++) {
            outputStream.write(newRows[i]);
        }
        outputStream.end();
        console.log(`${csvOutputFileName} written with stream and ${newRowCount} rows`);
    })
    .on("error", function (error) {
        console.log(error)
    });

