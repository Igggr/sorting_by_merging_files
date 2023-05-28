import fs from 'fs';
import readline from 'readline';
import * as uuid from 'uuid';
import * as path from 'path';


// файл 1 ТБ, оперативка 500 МБ ~ 200 меньше
// читаем по частям, сортируем часть и записываем в файл.
// потом сливаем файлы, выбирая меньшую из "первых оставшихся" строчек,
// как в merge-сортировки 
export async function mergeSort(
    inputFileName = 'input.txt',
    outputFileName = 'out.txt',
): Promise<void> {
    const files = await chunk(inputFileName);
    merge(files, outputFileName);
}

// есть 500 МБ, при использовании UTF-8 1 символ = 1 байт
// какова длинна строк - неизвестно, может быть даже весь файл это 1 большая строка
// тогда конечно ничего сделать не получится.
// далее буду считать, что макимальный размер 1 строки - 250 символов
// тогда в оперативку поместится 2000 строк
async function chunk(
    filePath: string,
    chunkSize = 2000,
): Promise<Set<string>> {
    // вобще-то тоже заниает оперативку
    const fileSet = new Set<string>();
    const fileStream = fs.createReadStream(filePath);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let counter = 0;
    let chunk = [];

    if (!fs.existsSync('tmp')) {
        fs.mkdirSync('tmp', { recursive: true });
    }

    for await (const line of rl) {
        if (counter < chunkSize) {
            counter++;
            chunk.push(line);
        } else {
            // если продолжать добавлять оперативка рискует переполниться
            chunk.sort();
            const fileName = path.resolve('tmp', uuid.v4());
            fileSet.add(fileName);
            const writable = fs.createWriteStream(
                fileName,
                { encoding: "utf-8", flags: "a" }
            );

            for (const line of chunk) {
                await writable.write(line);
            }
            // сбросить счетчик
            counter = 0;
            chunk = [];
        }
    }
    return fileSet;
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

async function merge(files: Set<string>, outputFileName: string): Promise<void> {
    const writeStream = fs.createWriteStream(
        outputFileName,
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