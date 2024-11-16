import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd
import requests
import json
import main
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import pandas_market_calendars as mcal
from market_cap_funct import *



load_dotenv()
apikey = os.environ.get("apikey")
tiingo_token = os.environ.get("tiingo_token")

# sp_500_stocks_1996 = pd.read_pickle("github_dataset.pkl")["date"]


def get_tiingo_meta_data_index():
    headers = {'Content-Type': 'application/json'}
    URL = "https://api.tiingo.com/tiingo/fundamentals/meta?token=" + tiingo_token
    requestResponse = requests.get(URL, headers=headers)
    meta_data_list = requestResponse.json()
    starting_indices = {}
    for index, stock in enumerate(meta_data_list):
        # if stock["name"] in [None, ""]:
        #     continue
        first_letter = stock['ticker'][0].upper()
        if first_letter not in starting_indices:
            starting_indices[first_letter] = index
    print(starting_indices)


tiingo_meta_data_index = {'A': 0, 'B': 1787, 'C': 2763, 'D': 4607, 'E': 5244, 'F': 6052, 'G': 6866, 'H': 7687, 'I': 8349, 
                          'J': 9170, 'K': 9383, 'L': 9730, 'M': 10382, 'N': 11485, 'O': 12269, 'P': 12752, 'Q': 13899, 'R': 14030,
                            'S': 14729, 'T': 16316, 'U': 17321, 'V': 17665, 'W': 18158, 'X': 18638, 'Y': 18773, 'Z': 18851}



# directory = 'company_profiles'
# sectors = {}
# industries = {}

# for filename in os.listdir(directory):
#     if filename.endswith('.json'):
#         with open(os.path.join(directory, filename), 'r') as file:
#             data = json.load(file)
#             sector = data["sector"]
#             industry = data["industry"]
#             exchange = data["exchange"]
#             if sectors.get(sector) != None : sectors[sector] += 1
#             else: sectors[sector] = 1

#             if industries.get(industry) != None : industries[industry] += 1
#             else: industries[industry] = 1

# sectors = dict(sorted(sectors.items(), key=lambda item: item[1], reverse=True))
# industries = dict(sorted(industries.items(), key=lambda item: item[1], reverse=True))

# print(sectors)
# print("\n")
# print(industries)

# print("\n")
# print(len(sectors), len(industries))

















# page_url = "https://companiesmarketcap.com/monsanto/marketcap/"
# response = requests.get(page_url)
# soup = BeautifulSoup(response.content, 'html.parser')
# # print(soup.prettify())
# data = soup.find("script",{"type": "text/javascript"}).string
# # type="text/javascript"

# # Use regex to find the data variable
# pattern = re.compile(r"data\s*=\s*(\[\{.*?\}\]);")
# match = pattern.search(data)
# if match:
#     data = match.group(1)
#     data = json.loads(data)
#     for i, point in enumerate(data):
#         unix_time, market_cap_in_millions = point["d"], point["m"]
#         market_cap_in_millions = point["m"]
#         del data[i]['m']
#         data[i]["marketcap"] = market_cap_in_millions * 1000000
#     print(data[:5], data[-5:])
# else:
#     print("Data not found")


# unix_time = int(datetime.strptime("2018-06-06" + " 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp())
# print(unix_time)

# unix_time = 971827200  # This is for illustration purposes
# # Convert Unix time to datetime object
# dt = datetime.fromtimestamp(unix_time, tz=timezone.utc)
# # Extract day, month, and year
# day = dt.day
# month = dt.month
# year = dt.year
# print(f"Month: {month}, Day: {day}, Year: {year}")
# print(dt.hour, dt.minute, dt.second)









# nyse = mcal.get_calendar('NYSE')

# date1 = datetime.strptime("2017-03-19", "%Y-%m-%d")
# date2 = datetime.strptime("2017-03-20", "%Y-%m-%d")

# print(nyse.valid_days(start_date=date1, end_date=date2))

# a = nyse.valid_days(start_date=date1, end_date=date2)

# # print(date1.__str__()[:10])

# print(nyse.valid_days(start_date=date1, end_date=date2)[0].date().__str__())

# print(a)
# a = a[1:]
# print(a)














finchat_download_dict = {545:(15,60), 670:(3,12), 680:(0,45)}
finchat_screenshot_dict = {586:(0,40), 595:(5,20), 609:(0,40), 612:(0,15), 649:(5,25), 652:(10,30), 659:(0, 250), 660:(0,140) 
                        ,662:(2,7), 679:(0, 70), 691:(2,14), 695:(0,70)}
#700s, fixed 767 image
finchat_download_dict.update({721:(1,14), 727:(2,26), 737:(0,11), 743:(7,14), 754:(5,13), 762:(0,24), 765:(3.75,6.5)
                            ,767:(0,14), 769:(0,8), 771:(2,24), 782:(2,18), 784:(3,14)})
finchat_screenshot_dict.update({714:(0,35), 716:(0,30), 718:(2,16), 731:(5,20), 733:(0,20), 744:(2,14), 747:(0,20), 749:(0,15)
                                ,750:(5,20), 752:(0,10), 753:(0,40), 756:(10,35), 757:(5,20), 758:(0,15), 759:(0,35)
                                ,761:(0,8), 770:(0,15), 774:(0,12), 775:(2,12), 776:(0.5,3), 780:(2,12), 781:(1,5)
                                ,786:(2,16), 787:(2,8), 790:(10,40), 791:(3,9), 797:(0,15)})
#800s
finchat_download_dict.update({806:(3,11), 861:(0,4.5), 867:(6,15), 868:(0,35), 883:(1,8)})
finchat_screenshot_dict.update({801:(2,12), 803:(0,10), 810:(4,16), 817:(2,14), 818:(2,12), 819:(20,100), 820:(0,120)
                                ,821:(5,30), 822:(4,8), 824:(1,7), 826:(0,40), 827:(20,55), 828:(1,5), 835:(10,20), 838:(2,9)
                                ,842:(5,40), 847:(5,30), 848:(1,4.5), 850:(0,10), 859:(2,6), 860:(10,30), 862:(1,5), 863:(1,5)
                                ,864:(0,8), 866:(0,40), 872:(5,11), 873:(4,8), 874:(2,14), 876:(10,25), 877:(0,7), 879:(0,40)
                                ,880:(5,20), 881:(4,16), 884:(4,14), 885:(15,30), 886:(0,30), 887:(4,7), 888:(1,2), 889:(2,10)
                                ,890:(10,22), 891:(0,20), 892:(2,8), 895:(1,4), 898:(4,16)})
#900s, changed 970 to 971, also in images
finchat_download_dict.update({901:(3,12), 903:(3,11), 904:(0.25,3.75), 905:(2,26), 910:(3,6.5), 911:(1.5,5), 914:(6,30)
                            ,915:(1,13), 916:(2,10), 917:(0,7), 918:(0,40), 919:(5,8.5), 922:(0,55), 923:(0,4), 925:(8,40)
                            ,927:(1,14), 931:(0,20), 932:(20,75), 933:(4,17), 937:(5,19), 939:(0.75,3.5), 941:(4,26)
                            ,943:(2,16), 947:(2,22), 948:(2,16), 949:(5,55), 950:(3,11), 952:(3,7.5), 953:(20,80), 955:(7,15)
                            ,956:(0,30), 958:(15,55), 960:(2,20), 962:(0.75,4), 968:(0.5,5), 969:(3,9), 973:(0,40), 982:(8,18)
                            ,989:(1.9,3.1), 990:(2,5), 993:(0.75,3.25), 994:(4,13), 995:(0.5,2.75), 997:(2.5,6.5), 999:(22,42)})
finchat_screenshot_dict.update({900:(1,5), 912:(10,25), 921:(0,15), 924:(2,14), 930:(0,14), 936:(4,12), 938:(2,10), 942:(0,8)
                                ,951:(0.5,4), 954:(0.5,4), 963:(0,2.5), 965:(10,35), 971:(12,20), 983:(0,14), 988:(2,7)})
#1000s, fixed 1078
finchat_download_dict.update({1000:(3,9), 1007:(5,14), 1011:(2,14), 1016:(2.5,6.5), 1017:(3,10), 1019:(4,22), 1022:(0.3,1.2)
                            ,1023:(0.3,1.3), 1025:(6,15), 1032:(3.8,5.6), 1033:(3,12), 1034:(2,20), 1036:(4,13), 1037:(3.5,6)
                            ,1038:(0.7,1.8), 1039:(6,10.5), 1040:(1.2,3.4), 1042:(10,21), 1043:(2.6,4.6), 1044:(0.7,1.7)
                            ,1046:(2.5,5.75), 1049:(1,7), 1052:(40,80), 1057:(2,8), 1060:(2.5,6.5), 1063:(1.75,5.25)
                            ,1064:(0.55,1.05), 1065:(2.25,5.25), 1066:(10,55), 1067:(16,34), 1073:(3.75,6.75), 1075:(3.5,8.5)
                            ,1078:(45,90), 1080:(1.4,3.2), 1081:(1,5.5), 1084:(0.4,1.3), 1085:(2.5,5.25), 1086:(0.7,2)
                            ,1087:(0.3,1.3), 1088:(40,90), 1089:(4,11), 1090:(7,18), 1091:(0,2.75), 1092:(3,11), 1094:(1.5,4)
                            ,1095:(0.2,1.8), 1097:(4,8), 1098:(5.5,10)})
finchat_screenshot_dict.update({1001:(20,70), 1003:(10,25), 1006:(2,10), 1009:(0.6,1.8), 1010:(2.5,5), 1013:(1,7), 1014:(0.6,1.8)
                                ,1018:(4,10), 1021:(1.5,5), 1045:(0.6,1.6), 1047:(5,40), 1050:(4,12), 1054:(1,5), 1059:(3,7)
                                ,1061:(1,2.5), 1069:(0.5,2), 1079:(5,9), 1082:(1.5,5), 1083:(2,9), 1093:(0.015,0.05)})
#1100s, fixed 1100, 1146
finchat_download_dict.update({1100:(15,70), 1103:(2.5,5.5), 1105:(4,14), 1106:(0.7,1.6), 1107:(5,11), 1108:(2,6.5), 1109:(0.4,2.2)
                            ,1110:(5,13), 1111:(3,6), 1113:(1,3), 1114:(8,17), 1117:(36,60), 1119:(14,21), 1120:(0.25,0.8)
                            ,1125:(1.8,4), 1126:(17,29), 1127:(4,10), 1128:(4,10), 1129:(1.4,2.7), 1130:(28,52), 1131:(4.5,8.5)
                            ,1133:(2.1,3), 1136:(1.9,2.7), 1137:(2.1,3.4), 1138:(3,9), 1140:(2,6.5), 1142:(4.5,9.5)
                            ,1146:(1.52,1.72), 1147:(0.43,0.7)})
finchat_screenshot_dict.update({1101:(40,120), 1102:(5,12), 1118:(1.5,3.5), 1121:(1,3), 1123:(40,90), 1132:(2,4.5), 1134:(3,5.5)
                                ,1135:(7,14), 1139:(0.8,1.6), 1141:(2.8,3.8), 1143:(14,22), 1144:(2.2,3.2), 1145:(0.4,0.6)
                                ,1150:(1.8,2.3)})





# print(len(list(finchat_download_dict)))

print(list(finchat_screenshot_dict))
#132 screenshots
#142 downloads
#correct # of total files: 274