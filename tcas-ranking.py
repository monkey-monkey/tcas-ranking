import pandas as pd
from numpy import nan
from queue import PriorityQueue

# User Input Data
path_user =
# Exam Statistic
path_exam =
# TCAS Data (Aka "project_data")
path_project =

df_user_data = pd.read_csv(path_user)
df_user_data = df_user_data[:]
dict_user = df_user_data.set_index("user_code").T.to_dict("list")
dict_user = eval(str(dict_user))
for user_code in list(dict_user.keys()):
    for i in range(len(dict_user[user_code])):
        tmp = dict_user[user_code][i]
        if type(tmp) == str:
            tmp = eval(tmp)
            dict_user[user_code][i] = tmp

df_exam = pd.read_csv(path_exam)
df_exam.columns = ["name", "id", "max_score",
                   "high_score", "min_score", "stdev", "mean"]
dict_exam = df_exam.set_index("name").T.to_dict("list")
dict_exam = eval(str(dict_exam))

df_project = pd.read_csv(path_project)
dict_project = df_project.set_index("id").T.to_dict("list")
dict_project = eval(str(dict_project))
for project_id in list(dict_project.keys()):
    for i in range(len(dict_project[project_id])):
        tmp = dict_project[project_id][i]
        if type(tmp) == str:
            try:
                tmp = eval(tmp)
            except:
                just_go = 1
            dict_project[project_id][i] = tmp

# SCORE CALCULATION FUNCTION


def calc_tscore(exam_key, user_score):
    sd = dict_exam[exam_key][4]
    mean = dict_exam[exam_key][5]
    tscore = 50 + 6.18606*(user_score-mean)/sd
    return tscore


def calc_score(user_code, project_id):
    value = dict_user[user_code]
    skip_list = ["vnet_513", "gpa22_23_28", ""]
    total_score = 0
    for exam_key, exam_percentage in dict_project[project_id][15].items():
        if exam_key in skip_list:
            continue

        if exam_key != "cal_type":
            max_score = dict_exam[exam_key][1]
            try:
                user_score = value[0][exam_key]
            except:
                continue

            if user_score > dict_exam[exam_key][2] or user_score < dict_exam[exam_key][3]:
                return -100

            if dict_project[project_id][18] == True:
                user_score = calc_tscore(exam_key, user_score)
                max_score = 100
            total_score += user_score/max_score*exam_percentage
        else:
            cal_max_score_sum = 0
            cal_user_score_sum = 0

            cal_subject_name_list = dict_project[project_id][15]["cal_subject_name"].strip(
                " ")
            cal_subject_name_list = cal_subject_name_list.replace("| ", "|")
            cal_subject_name_list = cal_subject_name_list.split(" ")
            for cal_exam_key in cal_subject_name_list:
                max_tmp_max = 0
                max_tmp_score = 0

                cal_inner_subject_name_list = cal_exam_key.split("|")
                for cal_inner_exam_key in cal_inner_subject_name_list:
                    if cal_inner_exam_key in skip_list:
                        continue

                    max_score = dict_exam[cal_inner_exam_key][1]
                    try:
                        user_score = value[0][cal_inner_exam_key]
                    except:
                        continue

                    if user_score > dict_exam[cal_inner_exam_key][2] or user_score < dict_exam[cal_inner_exam_key][3]:
                        return -100

                    if dict_project[project_id][18] == True:
                        user_score = calc_tscore(
                            cal_inner_exam_key, user_score)
                        max_score = 100

                    max_tmp_max = max(max_tmp_max, max_score)
                    max_tmp_score = max(max_tmp_score, user_score)

                cal_max_score_sum += max_tmp_max
                cal_user_score_sum += max_tmp_score

            if cal_max_score_sum:
                total_score += cal_user_score_sum/cal_max_score_sum * \
                    dict_project[project_id][15]["cal_score_sum"]
            break

    return total_score

# SET MAX STUDENT NUMBER PER PROJECT
multiplier = 34499/100000
receive_student_number = [0] * 20000
for key, value in dict_project.items():
    if value[17] != value[17]:
        receive_student_number[key] = value[13] * multiplier
    else:
        receive_student_number[key] = -1
        receive_student_number[10000 + int(value[17])] = value[13] * multiplier

# SET PROJECT ACCEPTED
project_apply = [0 for i in range(20000)]
project_accept = [PriorityQueue() for i in range(20000)]

# CALCULATE PROJECT APPLY
for code in dict_user:
    value = dict_user[code]
    for project_id in value[1]:
        if dict_project[project_id][17] != dict_project[project_id][17]:
            duplicates_list = dict_project[project_id][19]
            for dup_id in duplicates_list:
                project_apply[int(dup_id)] += 1
        else:
            this_join_id = dict_project[project_id][17]
            project_apply[10000 + int(this_join_id)] += 1

# INITIALIZE USERS STATUS
dict_user_status = {}
for code in dict_user:
    dict_user_status[code] = (0, -1)
# 0 Untouched
# 1 Still in some project
# 2 Done (Mai Tid Arai Leuy)

# BEGIN RANKING
final_done = len(dict_user)
while True:

    # RIP TIME COMPLEXITY O(N), (COULD BE DONE IN O(1))
    done = 0
    for user in dict_user_status:
        status, order = dict_user_status[user]
        if status != 0:
            done += 1
    if done == final_done:
        break

    # RIP TIME AND SPACE COMPLEXITY AGAIN
    tmp_dict_user_status = dict(dict_user_status)

    for code in dict_user:
        is_error = 0
        status, order = dict_user_status[code]
        if status != 0:
            continue

        project_list = dict_user[code][1]
        if order + 1 == len(project_list):
            tmp_dict_user_status[code] = (2, order + 1)

        for index in range(order+1, len(project_list)):

            project_id = project_list[index]

            # SKIP INVALID CASES
            if dict_project[project_id][12] != 1:
                if index + 1 == len(project_list):
                    tmp_dict_user_status[code] = (2, index)
                continue
            if len(dict_project[project_id][15]) == 0:
                if index + 1 == len(project_list):
                    tmp_dict_user_status[code] = (2, index)
                continue

            total_score = calc_score(code, project_id)
            if total_score == -100:
                if index + 1 == len(project_list):
                    tmp_dict_user_status[code] = (2, index)
                continue

            if dict_project[project_id][17] != dict_project[project_id][17]:
                duplicates_list = dict_project[project_id][19]

                if project_accept[project_id].qsize() < receive_student_number[project_id]:
                    for dup_id in duplicates_list:
                        project_accept[int(dup_id)].put(
                            (total_score, project_id, code))
                    tmp_dict_user_status[code] = (1, index)
                    break

                elif receive_student_number[project_id] != -1:
                    for dup_id in duplicates_list:
                        kicked_score, kicked_project_id, kicked_code = project_accept[dup_id].get(
                        )
                    kicked_status, kicked_order = tmp_dict_user_status[kicked_code]

                    if kicked_score > total_score:
                        for dup_id in duplicates_list:
                            project_accept[int(dup_id)].put(
                                (kicked_score, kicked_project_id, kicked_code))
                        if index + 1 == len(project_list):
                            tmp_dict_user_status[code] = (2, index)
                        continue
                    else:
                        for dup_id in duplicates_list:
                            project_accept[int(dup_id)].put(
                                (total_score, project_id, code))

                        tmp_dict_user_status[code] = (1, index)
                        tmp_dict_user_status[kicked_code] = (0, kicked_order)
                        break

            else:
                new_project_id = 10000 + int(dict_project[project_id][17])

                if project_accept[new_project_id].qsize() < receive_student_number[new_project_id]:
                    project_accept[new_project_id].put(
                        (total_score, project_id, code))
                    tmp_dict_user_status[code] = (1, index)
                    break

                elif receive_student_number[new_project_id] != -1:
                    kicked_score, kicked_project_id, kicked_code = project_accept[new_project_id].get(
                    )
                    kicked_status, kicked_order = tmp_dict_user_status[kicked_code]

                    if kicked_score > total_score:
                        project_accept[new_project_id].put(
                            (kicked_score, kicked_project_id, kicked_code))
                        if index + 1 == len(project_list):
                            tmp_dict_user_status[code] = (2, index)
                        continue
                    else:
                        project_accept[new_project_id].put(
                            (total_score, project_id, code))
                        tmp_dict_user_status[code] = (1, index)
                        tmp_dict_user_status[kicked_code] = (0, kicked_order)
                        break

    dict_user_status = dict(tmp_dict_user_status)

# EXPORT DATA
exported_project_accept = []
for i in range(20000):
    project_tmp = []
    while not project_accept[i].empty():
        total_score, project_id, code = project_accept[i].get()
        project_tmp.append((total_score, code))
    project_tmp = [project_tmp]
    exported_project_accept.append(project_tmp)

df_project_accept = pd.DataFrame(exported_project_accept)
end_project_accept_path = 
df_project_accept.to_csv(end_project_accept_path)

df_project_apply = pd.DataFrame(project_apply)
end_project_apply_path = 
df_project_apply.to_csv(end_project_apply_path)
