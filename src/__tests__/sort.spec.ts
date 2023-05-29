import * as fsPromises from "node:fs/promises";
import * as path from 'path';
import { getWriteStream, mergeSort } from "../main";

async function traditionalSort(inputFile: string, outputFile: string) {
    const f = await fsPromises.open(inputFile);
    const lines = [];
    for await (const line of f.readLines()) {
        if (line) {
            lines.push(line);
        }
    }
    lines.sort();
    
    const writer = getWriteStream(outputFile);
    for (const line of lines) {
        await writer.write(line);
        await writer.write('\n');
    }

    return lines;
}

describe('Should be able to sort file', () => {

    it('input.txt', async () => {
        const input = path.resolve(__dirname, 'input.txt');
        const output = path.resolve(__dirname, 'output.txt');
        const traditionalSortOutput = path.resolve(__dirname, 'output_expected.txt');

        await mergeSort(input, output, 5);
        await traditionalSort(input, traditionalSortOutput);

        const result = await fsPromises.readFile(output, { encoding: "utf-8" });
        const expected = await fsPromises.readFile(traditionalSortOutput, { encoding: "utf-8" });
        expect(result).toEqual(expected);

        fsPromises.rm(output);
        fsPromises.rm(traditionalSortOutput);
    })
})