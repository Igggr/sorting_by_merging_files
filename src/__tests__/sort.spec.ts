import * as fsPromises from "node:fs/promises";
import * as path from 'path';
import { mergeSort } from "../main"

async function traditionalSort(fileName: string) {
    const f = await fsPromises.open(fileName);
    const lines = [];
    for await (const line of f.readLines()) {
        lines.push(line);
    }
    return lines;
}

describe('Should be able to sort file', () => {

    it('input.txt', async () => {
        const input = path.resolve(__dirname, 'input.txt');
        const output = path.resolve(__dirname, 'output.txt');

        console.log('start merge sorting');
        await mergeSort(input, output, 5);
        console.log('sorted throuth merge')
        const result = fsPromises.readFile(output, { encoding: "utf-8" });
        const expected = traditionalSort(input);
        console.log('sorted throuth input')
        expect(result).toEqual(expected);
        
    })
})