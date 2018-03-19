import argparse
import os
import re
import copy
import upload_test_result
import big_query_utils

class Result:
    def __init__(self, filename, throughput, behavior, language):
        self.dataFileName = filename
        self.throughput = throughput
        self.behavior = behavior
        self.language = language

average_bytes_per_message = {
    "google_message2": 84570.0,
    "google_message1_proto2": 228.0,
    "google_message1_proto3": 228.0,
    "google_message4": 18687.277215502345,
    "google_message3_4": 16161.58529111338,
    "google_message3_3": 75.3473942530787,
    "google_message3_5": 20.979602347823803,
    "google_message3_2": 366554.9166666667,
    "google_message3_1": 200567668
}


cpp_result = []
python_result = []
java_result = []
go_result = []


def parse_cpp_result(filename):
    global cpp_result
    if filename == "":
        return
    if filename[0] != '/':
        filename = os.path.dirname(os.path.abspath(__file__)) + '/' + filename
    with open(filename) as f:
        for line in f:
            result_list = re.split("[\ \t]+", line)
            benchmark_name_list = re.findall("google_message[0-9]+.*$", result_list[0])
            if len(benchmark_name_list) == 0:
                continue
            filename = re.split("(_parse_|_serialize)", benchmark_name_list[0])[0]
            behavior = benchmark_name_list[0][len(filename) + 1:]
            throughput_with_unit = re.split('/s', result_list[-1])[0]
            if throughput_with_unit[-2:] == "GB":
                throughput = float(throughput_with_unit[:-2]) * 1024
            else:
                throughput = float(throughput_with_unit[:-2])
            cpp_result.append({
                "dataFilename": filename,
                "throughput": throughput,
                "behavior": behavior,
                "language": "cpp"
            })


def parse_python_result(filename):
    global average_bytes_per_message, python_result
    if filename == "":
        return
    if filename[0] != '/':
        filename = os.path.dirname(os.path.abspath(__file__)) + '/' + filename
    with open(filename) as f:
        result = {"language": "python"}
        for line in f:
            if line.find("./python") != -1:
                result["behavior"] = re.split("[ \t]+", line)[0][9:]
            elif line.find("dataset file") != -1:
                result["dataFileName"] = re.split(
                    "\.",
                    re.findall("google_message[0-9]+.*$", line)[0])[-2]
            elif line.find("Average time for ") != -1:
                elements = re.split("[ \t]+", line)
                new_result = copy.deepcopy(result)
                new_result["behavior"] += '_' + elements[3][:-1]
                new_result["throughput"] = \
                    average_bytes_per_message[new_result["dataFileName"]] / \
                    float(elements[4]) * 1e9 / 1024 / 1024
                python_result.append(new_result)


def parse_java_result(filename):
    global average_bytes_per_message, java_result
    if filename == "":
        return
    if filename[0] != '/':
        filename = os.path.dirname(os.path.abspath(__file__)) + '/' + filename
    with open(filename) as f:
        result = {"language": "java"}
        for line in f:
            if line.find("dataFile=") != -1:
                result["dataFileName"] = re.split(
                    "\.",
                    re.findall("google_message[0-9]+.*$", line)[0])[-2]
            if line.find("benchmarkMethod=") != -1:
                for element in re.split("[ \t,]+", line):
                    if element[:16] == "benchmarkMethod=":
                        result["behavior"] = element[16:]
            if line.find("median=") != -1:
                for element in re.split("[ \t,]+", line):
                    if element[:7] == "median=":
                        result["throughput"] = \
                            average_bytes_per_message[result["dataFileName"]] / \
                            float(element[7:]) * 1e9 / 1024 / 1024
                        java_result.append(copy.deepcopy(result))
                        continue


def parse_go_result(filename):
    global average_bytes_per_message, go_result
    if filename == "":
        return
    if filename[0] != '/':
        filename = os.path.dirname(os.path.abspath(__file__)) + '/' + filename
    with open(filename) as f:
        for line in f:
            result_list = re.split("[\ \t]+", line)
            benchmark_name_list = re.split("[/\.]+", result_list[0])
            filename = ""
            for s in benchmark_name_list:
                if s[:14] == "google_message":
                    filename = s
            if filename == "":
                continue
            behavior = benchmark_name_list[-1]
            throughput = \
                average_bytes_per_message[filename] / \
                float(result_list[-2]) * 1e9 / 1024 / 1024
            go_result.append({
                "dataFilename": filename,
                "throughput": throughput,
                "behavior": behavior,
                "language": "go"
            })


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-cpp", "--cpp_input_file",
                        help="The CPP benchmark result file's name",
                        default="")
    parser.add_argument("-java", "--java_input_file",
                        help="The Java benchmark result file's name",
                        default="")
    parser.add_argument("-python", "--python_input_file",
                        help="The Python benchmark result file's name",
                        default="")
    parser.add_argument("-go", "--go_input_file",
                        help="The golang benchmark result file's name",
                        default="")
    args = parser.parse_args()

    parse_cpp_result(args.cpp_input_file)
    parse_python_result(args.python_input_file)
    parse_java_result(args.java_input_file)
    parse_go_result(args.go_input_file)

    print cpp_result, python_result, java_result, go_result
    for result in cpp_result:
        new_result = copy.deepcopy(result)
        upload_test_result.populate_metadata_inplace(new_result)
        bq = big_query_utils.create_big_query()
        upload_test_result.insert_result(bq, "protobuf_benchmark_result", "opensource_result_v1", new_result)
