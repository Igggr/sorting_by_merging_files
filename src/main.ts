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
    await merge(files, outputFilePath);
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

    let chunk: string[] = [];
    let counter = 1; // на каком chunk-е остановились
    // let i = 1;

    if (!fs.existsSync('tmp')) {
        fs.mkdirSync('tmp', { recursive: true });
    }

    let writer: fs.WriteStream;
    for await (const line of rl) {
        if (!line) {
            continue;
        }
        // console.log(`${i++}:`, `'${line}'`);

        if (chunk.length < chunkSize) {
            chunk.push(line);
        } else {
            // если продолжать добавлять оперативка рискует переполниться
            // отсоритровать и записать во временный файл

            const tmpFilePath = getTmpFile(counter);
            fileSet.add(tmpFilePath);
            await writeChunk(chunk, tmpFilePath);
  
            counter++;
            chunk = [line];
        }
    }
    
    // @ts-ignore
    if (chunk.length > 0) {
        const tmpFilePath = getTmpFile(counter);
        fileSet.add(tmpFilePath);
        writeChunk(chunk, tmpFilePath);
    }

    return fileSet;
}

async function writeChunk(chunk: string[], filePath: string) {
    // кажется зависит от локали. сейчас только для английского
    chunk.sort();
    const writer = getWriteStream(filePath);
    for (const line of chunk) {
        await writer.write(line);
        await writer.write('\n');
    }
    await finish(writer);
}

function getTmpFile(counter: number) {
    return path.resolve('tmp', `${counter}.txt`);
}

export function getWriteStream(filePath: string) {
    const nodeWritable = fs.createWriteStream(
        filePath,
        { encoding: "utf-8", flags: 'a' }
    );
    return nodeWritable;
}

async function finish(stream: fs.WriteStream): Promise<void> {
    stream.close();
    return new Promise((resolve, reject) => {
        stream.once('close', () => {
            resolve();
        });
    });
}

class Stream {
    private line: string | null = null;
    private iter: AsyncGenerator<string, void, unknown>;
    private rl: readline.Interface;
    private done: boolean = false;

    constructor(private readonly stream: fs.ReadStream) {
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });
        this.rl = rl;
        this.iter = (async function* () {
            for await (const line of rl) {
                yield line
            }
        })()
    }

    get isClosed() {
        return this.line === null && this.done;
    }

    async read() {
        if (this.line !== null) {
            return this.line;
        }
        if (this.isClosed) {
            return null;
        }
        const res = await this.iter.next();
        if (res.done) {
            this.done = true;
            this.rl.close();
            return null;
        }
        this.line = res.value;
        return this.line;
    }

    take() {
        const line = this.line;
        this.line = null;
        return line;
    }
}

type SortEntity = {
    stream: Stream;
    line: string;
}

async function merge(files: Set<string>, outputFilePath: string): Promise<void> {
    // console.log("startig sort")
    const writeStream = getWriteStream(outputFilePath);

    const streams = Array.from(files).map((fileName) => {
        const filePath = path.resolve('tmp', fileName);
        return new Stream(fs.createReadStream(filePath));
    })

    const hasLines = () => streams.some((s) => !s.isClosed);

    // неэффективно. надо бы min-дерево
    async function getMinLine() {
        const lines = (await Promise.all(
            streams.filter((stream) => !stream.isClosed)
                .map(async (stream, index) => ({ line: await stream.read(), stream }))
        )).filter((se) => !!se)
            .filter((se) => se.line !== null) as SortEntity[];

        if (lines.length === 0) {
            return null;
        }
        const min = lines
            .reduce((min: SortEntity, entry: SortEntity) =>
                entry.line <= min.line ? entry : min, lines[0]
            );

        min.stream.take();
        return min.line;

    }

    let i = 0;

    while (hasLines()) {
        const line = await getMinLine();
        if (line === null) {
            continue;
        }
        // console.log('minLIne', i++, line)
        writeStream.write(line);
        writeStream.write('\n');
    }

    finish(writeStream);
}

async function clear(fileNames: Set<string>) {
    for (const fileName of fileNames) {
        await fsPromises.rm(fileName)
    }
}