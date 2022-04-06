import re
import json
import pandas as pd
from datetime import datetime
from openpyxl.workbook import Workbook


def epoch_to_datetime_converter(epoch_time):
    datetime_obj = datetime.utcfromtimestamp(epoch_time)
    return datetime_obj


with open("./messages.json") as f:
    messages_all = json.load(f)

result = []
n = 1
for item in messages_all:
    slownik = {}
    slownik['rn'] = n
    if 'subtype' in item.keys():
        slownik['subtype'] = item['subtype']
    if 'bot_id' in item.keys():
        slownik['bot_id'] = item['bot_id']
    slownik['temp_text'] = item['text']
    slownik['message_date'] = epoch_to_datetime_converter(int(item['ts'][:-7])).date()
    slownik['message_GMT'] = epoch_to_datetime_converter(int(item['ts'][:-7])).time()
    if 'attachments' in item.keys():
        slownik['temp_attachments'] = item['attachments']
        # print(item['attachments'])
        # print(item['attachments'][0]['fallback'])

        regex_a_title = r"(?<=] )(.+)(?= \| )"
        matches_a_title = re.findall(regex_a_title, item['attachments'][0]['fallback'], re.MULTILINE)
        if len(matches_a_title) >= 5:
            slownik['a_title'] = matches_a_title
        else:
            slownik['a_title'] = item['attachments'][0]['fallback']

        regex_a_firing = r"(?<=\[FIRING:)(\d+)(?=\] )"
        matches_a_firing = re.findall(regex_a_firing, item['attachments'][0]['fallback'], re.MULTILINE)
        if len(matches_a_firing) == 0:
            matches_a_firing = [""]
        slownik['a_firing_number'] = matches_a_firing[0]

        regex_a_url = r"(?<= <)(http:\/\/.*)(?=>)"
        matches_a_url = re.findall(regex_a_url, item['attachments'][0]['fallback'], re.MULTILINE)
        if len(matches_a_url) == 0:
            matches_a_url = [""]
        slownik['a_url'] = matches_a_url[0]

        if 'text' in item['attachments'][0].keys():
            # print(item['attachments'][0]['text'])
            regex_a_cluster = r"(?<='\*cluster:\*)(\D+)(?=\* )"
            matches_a_cluster = re.findall(regex_a_cluster, item['attachments'][0]['text'], re.MULTILINE)
            if len(matches_a_cluster) == 0:
                matches_a_cluster = [""]
            slownik['a_cluster'] = matches_a_cluster[0]

            # print(item['attachments'][0]['text'])
            regex_a_description = r"(?<=\*description:\* )(.+)"
            matches_a_description = re.findall(regex_a_description, item['attachments'][0]['text'], re.MULTILINE)
            listToStr = ', '.join([str(elem) for elem in matches_a_description])
            # print(listToStr)
            slownik['a_description'] = listToStr

            regex_a_summary = r"(?<=\*summary:\* )(.+)"
            matches_a_summary = re.findall(regex_a_summary, item['attachments'][0]['text'], re.MULTILINE)
            listToStr = ', '.join([str(elem) for elem in matches_a_summary])
            # print(listToStr)
            slownik['a_summary'] = listToStr

    if 'text' in item.keys():
        textA = item['text'].replace("\n", "@@@")
        textB = "@@@"
        text = textA+textB

        regex1 = r"(?<=^\*)(.*)(?=\*)"
        matches1 = re.findall(regex1, item['text'], re.MULTILINE)
        regex2 = r"(?<=: ).*"
        matches2 = re.findall(regex2, item['text'], re.MULTILINE)

        regex_title = r"(?<=^).*?(?=@@@)"
        matches_title = re.findall(regex_title, text)
        slownik['title'] = matches_title[0]

        if "DAG" in matches1:
            regex_dag = r"(?<=DAG\*: `)(.*?)(?=`@@@)"
            matches_dag = re.findall(regex_dag, text, re.MULTILINE)
            slownik['DAG_name'] = matches_dag[0]

        if "Priority" in matches1:
            regex_priority = r"(?<=Priority\*: )(.*?)(?=@@@)"
            matches_priority = re.findall(regex_priority, text, re.MULTILINE)
            slownik['full_priority'] = matches_priority[0][1:-1]
            regex_priority = r"P[0-9]"
            matches_priority = re.findall(regex_priority, matches_priority[0][1:-1], re.MULTILINE)
            slownik['priority'] = matches_priority[0]

        if "Last Known Task" in matches1:
            regex_last_known_task = r"(?<=Last Known Task\*: <)(.*?)(?=>@@@)"
            matches_last_known_task = re.findall(regex_last_known_task, text, re.MULTILINE)
            slownik['DAG_url'] = matches_last_known_task[0]

        if "Task" in matches1:
            regex_task = r"(?<=Task\*: <)(.*?)(?=>@@@)"
            matches_task = re.findall(regex_task, text)
            slownik['DAG_url_2'] = matches_task[0]

        if "Reason" in matches1:
            regex_reason = r"(?<=Reason\*: )(.*)(?=@@@)"
            matches_reason = re.findall(regex_reason, text, re.MULTILINE)
            if len(matches_reason) == 0:
                matches_reason = ["UNPACK_ERROR"]
            slownik['reason'] = matches_reason[0].replace("@@@", "")

        if "Error" in matches1:
            regex_error = r"(?<=Error\*: )(.*?)(?=@@@)"
            matches_error = re.findall(regex_error, text, re.MULTILINE)
            slownik['DAG_owner_reason'] = matches_error[0]

        if "Owner" in matches1:
            regex_owner = r"(?<=Owner\*: )(.*?)(?=@@@)"
            matches_owner = re.findall(regex_owner, text, re.MULTILINE)
            slownik['DAG_owner_reason_2'] = matches_owner[0]

        if "Time" in matches1:
            regex_time = r"(?<=Time\*: )(.*?)(?=@@@)"
            matches_time = re.findall(regex_time, text, re.MULTILINE)
            slownik['time'] = matches_time[0]

        if "Runbook" in matches1:
            regex_runbook = r"(?<=Runbook\*: <)(.*?)(?=>@@@)"
            matches_runbook = re.findall(regex_runbook, text, re.MULTILINE)
            slownik['runbook'] = matches_time[0]

    n += 1
    result.append(slownik)


print(f"The dictionary has {len(result)} items.")
df = pd.DataFrame(result)
print(df.count())
df.to_excel(excel_writer="output8000_final.xlsx")