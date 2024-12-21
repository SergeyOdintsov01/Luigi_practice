import luigi
import os
import wget
import tarfile
import gzip
import shutil
import pandas as pd
import io

class BaseTask(luigi.Task):
    # Имя датасета подаем в качестве параметра
    dataset_name = luigi.Parameter(default="GSE68849")  
    # Монтируем директорию для хранения скачанного архива GSE68849.tar
    output_dir = luigi.Parameter(default="./dataset")   

# Шаг 1: Скачиваем архив 
class DownloadDataset(BaseTask):
    def output(self):
        # LocalTarget - определяет объект выхода задачи в виде файла на локальной машине. 
        # Задача будет считаться выполненной, когда этот объект существует. 
        return luigi.LocalTarget(f"{self.output_dir}/{self.dataset_name}_RAW.tar")

    def run(self):
        # Создаём директорию, если её нет
        os.makedirs(self.output_dir, exist_ok=True)
        # Формируем ссылку для скачивания архива
        url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{self.dataset_name[:-3]}nnn/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar"
        # Скачиваем архив с помощью утилиты wget
        wget.download(url, self.output().path)


# Шаг 2: Распаковка GSE68849.tar архива
class ExtractMainArchive(BaseTask):
    def requires(self):
        # Указываем, что эта задача зависит от скачивания архива
        return DownloadDataset(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        # Директория для извлеченных файлов
        return luigi.LocalTarget(f"{self.output_dir}/extracted")

    def run(self):
        # Распаковываем tar-архив
        with tarfile.open(self.input().path, "r") as tar:
            tar.extractall(self.output().path)


# Шаг 2.1: Распаковка каждого .gz файла в отдельную папку
class ExtractFiles(BaseTask):
    def requires(self):
        # Зависимость от распаковки основного архива
        return ExtractMainArchive(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        # Директория для сохранения распакованных файлов
        return luigi.LocalTarget(f"{self.output_dir}/processed")

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)  # Создаём папку, если её нет
        extracted_dir = self.input().path  # Директория с извлечённым содержимым архива

        # Перебираем все файлы в папке
        for file in os.listdir(extracted_dir):
            if file.endswith(".gz"):  # Проверяем, что файл имеет расширение .gz
                with gzip.open(os.path.join(extracted_dir, file), "rb") as f_in:
                    file_name = file.replace(".gz", "")  # Убираем расширение .gz из имени файла
                    file_dir = os.path.join(self.output().path, file_name.split(".")[0])
                    os.makedirs(file_dir, exist_ok=True)  # Создаём папку для файла
                    # Распаковываем содержимое файла
                    with open(os.path.join(file_dir, file_name), "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)


# Шаг 2.2: Разделение содержимого файлов на таблицы
class SplitTables(BaseTask):
    def requires(self):
        # Зависимость от распаковки файлов .gz
        return ExtractFiles(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        # Директория для сохранения таблиц
        return luigi.LocalTarget(f"{self.output_dir}/tables")

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)  # Создаём папку, если её нет
        processed_dir = self.input().path  # Директория с распакованными файлами

        # Обходим все папки
        for folder in os.listdir(processed_dir):
            folder_path = os.path.join(processed_dir, folder)
            for file in os.listdir(folder_path):
                if file.endswith(".txt"):  # Если файл имеет расширение .txt
                    dfs = {}
                    with open(os.path.join(folder_path, file), "r") as f:
                        write_key = None
                        fio = io.StringIO()
                        # Разделяем таблицы на основе хедеров
                        for line in f:
                            if line.startswith("["):  # Если строка начинается с "["
                                if write_key:
                                    fio.seek(0)
                                    dfs[write_key] = pd.read_csv(fio, sep="\t")
                                fio = io.StringIO()
                                write_key = line.strip("[]\n")
                                continue
                            if write_key:
                                fio.write(line)
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep="\t")

                    # Сохраняем таблицы в отдельные .tsv файлы
                    for key, df in dfs.items():
                        table_dir = os.path.join(self.output().path, folder)
                        os.makedirs(table_dir, exist_ok=True)
                        df.to_csv(os.path.join(table_dir, f"{key}.tsv"), sep="\t", index=False)


# Шаг 3: Чистка таблицы Probes. 
class FilterProbes(BaseTask):
    def requires(self):
        return SplitTables(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        # Директория для "очищенных" данных
        return luigi.LocalTarget(f"{self.output_dir}/filtered")

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)  # Создаём папку, если её нет
        tables_dir = self.input().path  # Директория с таблицами

        # Обходим все папки с таблицами
        for folder in os.listdir(tables_dir):
            folder_path = os.path.join(tables_dir, folder)
            for file in os.listdir(folder_path):
                if "Probes" in file:  # Ищем файл с таблицей Probes
                    df = pd.read_csv(os.path.join(folder_path, file), sep="\t")
                    # Удаляем ненужные колонки, согласно тз
                    filtered_df = df.drop(
                        columns=[
                            "Definition",
                            "Ontology_Component",
                            "Ontology_Process",
                            "Ontology_Function",
                            "Synonyms",
                            "Obsolete_Probe_Id",
                            "Probe_Sequence",
                        ],
                        errors="ignore",
                    )
                    # Сохраняем "очищенный" файл
                    filtered_dir = os.path.join(self.output().path, folder)
                    os.makedirs(filtered_dir, exist_ok=True)
                    filtered_df.to_csv(
                        os.path.join(filtered_dir, "Probes_filtered.tsv"),
                        sep="\t",
                        index=False,
                    )


# Шаг 4: Изначальный текстовый файл можно удалить, 
# убедившись, что все предыдущие этапы успешно выполнены.
class Cleanup(BaseTask):
    def requires(self):
        # Cleanup зависит от завершения задачи FilterProbes
        return FilterProbes(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        # Финальная директория, где будут сохранены обработанные данные
        return luigi.LocalTarget(f"{self.output_dir}/finalized")

    def run(self):
        # Проверяем, существует ли директория для финальных данных, если да — удаляем её
        if os.path.exists(self.output().path):
            print(f"Directory {self.output().path} already exists. Removing...")
            shutil.rmtree(self.output().path)  # Удаляем существующую директорию

        # Создаем директорию для финальных данных
        os.makedirs(self.output().path, exist_ok=True)

        # Удаляем временные данные, если они существуют
        temp_dirs = [
            os.path.join(self.output_dir, "extracted"),
            os.path.join(self.output_dir, "processed"),
            os.path.join(self.output_dir, "tables"),
        ]
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                print(f"Removing temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir, ignore_errors=True)

        # Копируем содержимое из предыдущего шага (FilterProbes) в финальную директорию
        print(f"Copying filtered data to {self.output().path}")
        shutil.copytree(self.input().path, self.output().path)

        print("Cleanup task completed successfully.")

# Запуск пайплайна
if __name__ == "__main__":
    luigi.run()
