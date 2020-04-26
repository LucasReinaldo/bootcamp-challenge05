import csvParse from 'csv-parse';
import fs from 'fs';
import { getCustomRepository, getRepository, In } from 'typeorm';
import Transaction from '../models/Transaction';
import TransactionsRepository from '../repositories/TransactionsRepository';
import Category from '../models/Category';

interface CSVTransaction {
  title: string;
  type: 'income' | 'outcome';
  value: number;
  category: string;
}

class ImportTransactionsService {
  async execute(filePath: string): Promise<Transaction[]> {
    const transactionsRepository = getCustomRepository(TransactionsRepository);
    const categoriesRepository = getRepository(Category);
    /** estou enviando no execute o arquivo e pelo
     * file system (fs) e filePath estou lendo esse arquivo
     */
    const contactReadStream = fs.createReadStream(filePath);

    /** do csv parse, eu instancio o csvParse para que ele leia a partir da segunda linha
     * se fosse separação por ; eu poderia passar delimiter: ';' para que fosse possível
     * lear o arquivo.
     */
    const parsers = csvParse({
      from_line: 2,
    });

    /** conforme a linha estiver disponível para leitura, o contactReadStream vai ler.
     */
    const parseCSV = contactReadStream.pipe(parsers);

    /** Variáveis temporárias para manipulação dos dados.
     */
    const tmpTransactions: CSVTransaction[] = [];
    const tmpCategories: string[] = [];

    /** 'data': nome do evento, e a cada linha, vamos desestruturar os valores
     * e cada célula, vamos remover os espaços no começo e final.
     */
    parseCSV.on('data', async line => {
      const [title, type, value, category] = line.map((cell: string) =>
        cell.trim(),
      );

      // caso não possua um deles não será lido.
      if (!title || !type || !value) return;

      /** precisamos salvar esses dados, mas para cada linha ele abre uma conexão e fecha
       * desse jeito salvamos temporariamento e depois enviamos, pois primeiro mapeamos e depois
       * mandamos. (Bulk insert).
       */
      tmpCategories.push(category);
      tmpTransactions.push({ title, type, value, category });
    });

    /** como parseCSV não é assíncrono, precisamos fazer com que fique rodando até
     * todo o arquivo ser lido, para que toda a info seja salva no array e depois
     * enviada pro DB, só termina quando o evento end é disparado.
     */
    await new Promise(resolve => parseCSV.on('end', resolve));

    /** verificar no array o títulos das categorias, se já existem.
     */
    const existentCategories = await categoriesRepository.find({
      where: {
        title: In(tmpCategories),
      },
    });

    /** verificar no array o títulos das categorias, se já existem através do existentCategories
     * se existe ele vai buscar o nome da categoria.
     */
    const existentCategoriesTitle = existentCategories.map(
      (category: Category) => category.title,
    );

    /** para cada categoria, no array temp, eu vou filtrar apenas aquelas em que
     * o title não for encontrado pela existentCategoriesTitle e vou incluir no meu retorno.
     * Já o segundo filter, remove as categorias duplicadas que serão salvas no DB.
     * Primeiro filtro todas que não possuo, depois filtro as duplicadas retornando apenas uma delas.
     */
    const addCategoryTitle = tmpCategories
      .filter(category => !existentCategoriesTitle.includes(category))
      .filter((value, index, self) => self.indexOf(value) === index);

    /**
     * salva todas as categorias em newCategories.
     */
    const newCategories = categoriesRepository.create(
      addCategoryTitle.map(title => ({
        title,
      })),
    );

    /**
     * salva no DB todas as categorias de newCategories.
     */

    await categoriesRepository.save(newCategories);

    /**
     * pega todas as categorias existentes e os novas categorias.
     */
    const allCategories = [...newCategories, ...existentCategories];

    /**
     * cria a instancia através das tmpTransactions e realiza o map em cada uma, pegando
     * title, type, value e em category, é feito uma busca em todas as categorias cadastradas
     * aonde o category title for igual ao category da transaction que está sendo importado.
     */
    const transactions = transactionsRepository.create(
      tmpTransactions.map(transaction => ({
        title: transaction.title,
        type: transaction.type,
        value: transaction.value,
        category: allCategories.find(
          category => category.title === transaction.category,
        ),
      })),
    );

    await transactionsRepository.save(transactions);

    /** remove o arquivo após importação */
    await fs.promises.unlink(filePath);

    return transactions;
  }
}

export default ImportTransactionsService;
