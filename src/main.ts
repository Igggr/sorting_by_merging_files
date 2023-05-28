import fs from 'node:fs';
import * as fsPromises from "node:fs/promises";
import readline from 'readline';
import * as path from 'path';
import { Writable, Readable } from "node:stream";


// файл 1 ТБ, оперативка 500 МБ ~ 200 меньше
// читаем по частям, сортируем часть и записываем в файл.
// потом сливаем файлы, выбирая меньшую из "первых оставшихся" строчек,
// как в merge-сортировки 

// есть 500 МБ, при использовании UTF-8 1 символ = 1 байт
// какова длинна строк - неизвестно, может быть даже весь файл это 1 большая строка
// тогда конечно ничего сделать не получится.
// далее буду считать, что макимальный размер 1 строки - 250 символов
// тогда в оперативку поместится 2000 строк
export async function mergeSort(
    inputFilePath = 'input.txt',
    outputFilePath = 'out.txt',
    chunkSize = 2000,
): Promise<void> {
    const files = await writeChunks(inputFilePath, chunkSize);
    merge(files, outputFilePath);
    await clear(files);
}

async function writeChunks(
    filePath: string,
    chunkSize: number,
): Promise<Set<string>> {
    // вобще-то тоже занимает оперативку
    const fileSet = new Set<string>();

    const fileStream = fs.createReadStream(filePath);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let chunk = [];
    let counter = 1; // на каком chunk-е остановились
    let i = 1;

    if (!fs.existsSync('tmp')) {
        fs.mkdirSync('tmp', { recursive: true });
    }

    for await (const line of rl) {
        if (!line) {
            continue;
        }
        console.log(`${i++}:`, `'${line}'`);
        
        if (chunk.length < chunkSize) {
            chunk.push(line);
        } else {
            // если продолжать добавлять оперативка рискует переполниться
            // отсоритровать и записать во временный файл

            chunk.sort();
            const tmpFilePath = getTmpFile(counter);
            fileSet.add(tmpFilePath);
            const writer = getWriteStream(tmpFilePath);

            try {
                console.log('formed chunk:', chunk)
                for (const line of chunk) {
                    await writer.write(line);
                    await writer.write('\n');
                }
                counter++;
                chunk = [];
                await finish(writer);
                console.log('closed: ', tmpFilePath)
            } catch (e) { 
                console.log(e);
            } 
        }
    }
    rl.close();

    return fileSet;
}

function getTmpFile(counter: number) {
    return path.resolve('tmp', `${counter}.txt`);
}

function getWriteStream(filePath: string) {
    const nodeWritable = fs.createWriteStream(
        filePath,
        { encoding: "utf-8", flags: 'a' }
    );
    return nodeWritable;
}

async function finish(stream: fs.WriteStream ): Promise < void>  {
    stream.close();
    return new Promise((resolve, reject) => {
        stream.once('close', () => {
            resolve();
        });
    });
}

class Stream {
    private line: string | null = null;
    constructor(private readonly stream: fs.ReadStream) { }
    
    isOpen() {
        if (this.line !== null) {
            return false;
        }
        return !this.stream.closed;
    }

    read() {
        if (this.line !== null) {
            return this.line;
        }
        
    }

    take() {
        const line = this.line;
        this.line = null;
        return line;
    }
}

type SortEntity = {
    index: number;
    line: string;
}

function merge(files: Set<string>, outputFilePath: string): void {
    console.log("startig sort")
    const writeStream = fs.createWriteStream(
        outputFilePath,
        { encoding: "utf-8", flags: "a" }
    );

    const streams = Array.from(files).map((fileName) => {
        const filePath = path.resolve('tmp', fileName);
        return new Stream(fs.createReadStream(filePath));
    })

    const hasLines = () => streams.length > 0;

    // неэффективно. надо бы min-дерево
    function getMinLine() {
        const min = streams.map((stream, index) => ({ line: stream.read(), index }) as SortEntity)
            .reduce((min: SortEntity, entry: SortEntity) =>
                entry.line <= min.line ? entry : min, { line: '', index: 0 }
        );
        const stream = streams[min.index];
        stream.take();
        if (!stream.isOpen) {
            streams.splice(min.index, 1);
        }
        return min.line;

    }

    while (hasLines()) {
        const line = getMinLine();
        writeStream.write(line);
    }
}

async function clear(fileNames: Set<string>) {
    for (const fileName of fileNames) {
        await fsPromises.rm(fileName)
    }
}