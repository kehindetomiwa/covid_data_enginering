import csv
import os

def partition_csv(inputfile, outputdir, prefix, record_per_csv):
    '''
    function partitions csv file into mutiple csv,
    this allows for parallel processing
    :param inputfile:
    :param outputdir:
    :param prefix:
    :param record_per_csv:
    :return:
    '''
    if record_per_csv <= 0:
        raise Exception('record must be > 0')
    with open(inputfile, 'r') as sfile:
        reader = csv.reader(sfile)
        headers = next(reader)

        idx = 0
        rexits = True

        while rexits:
            i = 0
            target_filename = f'{prefix}_{idx}.csv'
            target_path = os.path.join(outputdir, target_filename)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            with open(target_path, 'w') as target:
                writer = csv.writer(target)
                while i < record_per_csv:
                    if i == 0:
                        writer.writerow(headers)
                    try:
                        writer.writerow(next(reader))
                        i +=1
                    except:
                        rexits = False
                        break
            if i == 0:
                os.remove(target_path)
            idx += 1

def main():
    partition_csv('../../sampledata/covid_us_county.csv', '../../sampledata/covidus/', 'covidus_part', 2000)
    partition_csv('../../sampledata/us_county.csv', '../../sampledata/uscounty', 'uscounty_part' ,1000)


if __name__ == '__main__':
    main()