import pandas as pd

csv_extension = '.csv'
excel_extension = '.xlsx'

target_path = '/home/admin/PycharmProjects/csv_parsing/data_parsed/'
source_path = '/home/admin/PycharmProjects/csv_parsing/data/'

csv_2014_files = ['2014년 ' + str(x) + '분기' for x in [1, 2, 3, 4]]
csv_2015_files = ['2015년' + str(x) + '분기' for x in [1, 2, 3, 4]]
csv_2016_files = ['2016년 ' + str(x) + '분기' for x in [1, 2, 3, 4]]
csv_files_CP949 = csv_2014_files + csv_2016_files

for n, csv_file in enumerate(csv_files_CP949, 1):
  print(f'{n} th csv_file...(2014, 2016)')
  data = pd.read_csv(source_path + csv_file + csv_extension,
                   dtype= {'지역' : str, '측정소코드' : int, '측정소명' : str, '측정일시' : str, 'SO2' : float, 'CO' : float, 'O3' : float, 'NO2' : float, 'PM10' : 'Int64', 'PM25' : 'Int64', '주소' : str},
                   encoding='CP949')
  parsed_data = data[(data['지역'] == '부산 사하구') | (data['지역'] == '경남 하동군') | (data['지역'] == '인천 남동구')]
  parsed_data.to_csv(target_path + csv_file + csv_extension, float_format='%.3f', index=False)

for n, csv_file in enumerate(csv_2015_files, 1):

  print(f'{n} th csv_file...(2015)')
  data = pd.read_csv(source_path + csv_file + csv_extension,
                   dtype= {'지역' : str, '측정소코드' : int, '측정소명' : str, '측정일시' : str, 'SO2' : float, 'CO' : float, 'O3' : float, 'NO2' : float, 'PM10' : 'Int64', 'PM25' : 'Int64', '주소' : str})
  parsed_data = data[(data['지역'] == '부산 사하구') | (data['지역'] == '경남 하동군') | (data['지역'] == '인천 남동구')]
  parsed_data.to_csv(target_path + csv_file + csv_extension, float_format='%.3f', index=False)

excel_2013_files = ['2013년0' + str(x) + '분기' for x in [1 , 2, 3, 4]]
excel_2017_files = ['2017년 ' + str(x) + '월' for x in range(1, 13)]
excel_2018_files = ['2018년 ' + str(x) + '분기' for x in [1, 2, 3, 4]]
excel_files = excel_2013_files + excel_2017_files + excel_2018_files

for n, excel_file in enumerate(excel_files, 1):
  print(f'{n} th excel_file...(2013, 2017, 2018)')
  data = pd.read_excel(source_path + excel_file + excel_extension,
                       dtype= {'지역' : str, '측정소코드' : int, '측정소명' : str, '측정일시' : str, 'SO2' : float, 'CO' : float, 'O3' : float, 'NO2' : float, 'PM10' : 'Int64', 'PM25' : 'Int64', '주소' : str})
  parsed_data = data[(data['지역'] == '부산 사하구') | (data['지역'] == '경남 하동군') | (data['지역'] == '인천 남동구')]
  parsed_data.to_csv(target_path + excel_file + csv_extension, float_format='%.3f', index=False)

