import json

import pandas as pd
import seaborn as sns
import numpy as np

sns.set()
sns.set_style("whitegrid")
import matplotlib.pyplot as plt  # noqa


def report_execution_time(awslambda, pythonshell, pyspark):
    times = {"Workload Size (MB)": [], "Execution Time (s)": [], "Approaches": []}
    for size in pyspark.keys():
        times["Workload Size (MB)"].append(int(size))

        times["Workload Size (MB)"].append(int(size))
        if int(size) < 1024:
            times["Execution Time (s)"].append(float(awslambda[size]["ExecutionTime"]))
        else:
            times["Execution Time (s)"].append(None)
        times["Approaches"].append("AWS Lambda + AWS Wrangler")

        times["Execution Time (s)"].append(float(pythonshell[size]["ExecutionTime"]))
        times["Approaches"].append("AWS Glue + AWS Wrangler")

        times["Workload Size (MB)"].append(int(size))
        times["Execution Time (s)"].append(float(pyspark[size]["ExecutionTime"]))
        times["Approaches"].append("AWS Glue + Pyspark")

    df = pd.DataFrame(times)
    plt.figure()
    sns.barplot(
        x="Workload Size (MB)", y="Execution Time (s)", hue="Approaches", data=df
    )
    plt.yticks(np.arange(0, 350, 25))
    plt.savefig("docs/source/_static/report_execution_time.png")


def report_total_time(awslambda, pythonshell, pyspark):
    times = {
        "Workload Size (MB)": [],
        "Warm Up + Execution Time (s)": [],
        "Approaches": [],
    }
    for size in pyspark.keys():
        times["Workload Size (MB)"].append(int(size))

        times["Workload Size (MB)"].append(int(size))
        if int(size) < 1024:
            times["Warm Up + Execution Time (s)"].append(
                float(awslambda[size]["TotalElapsedTime"])
            )
        else:
            times["Warm Up + Execution Time (s)"].append(None)
        times["Approaches"].append("AWS Lambda + AWS Wrangler")

        times["Warm Up + Execution Time (s)"].append(
            float(pythonshell[size]["TotalElapsedTime"])
        )
        times["Approaches"].append("AWS Glue + AWS Wrangler")

        times["Workload Size (MB)"].append(int(size))
        times["Warm Up + Execution Time (s)"].append(
            float(pyspark[size]["TotalElapsedTime"])
        )
        times["Approaches"].append("AWS Glue + Pyspark")

    df = pd.DataFrame(times)
    plt.figure()
    sns.barplot(
        x="Workload Size (MB)",
        y="Warm Up + Execution Time (s)",
        hue="Approaches",
        data=df,
    )
    plt.yticks(np.arange(0, 1200, 100))
    plt.savefig("docs/source/_static/report_total_time.png")


def report_cost(awslambda, pythonshell, pyspark):
    times = {"Workload Size (MB)": [], "Cost (USD)": [], "Approaches": []}
    for size in pyspark.keys():
        times["Workload Size (MB)"].append(int(size))

        lambda_exec_time = float(awslambda[size]["ExecutionTime"])
        lambda_exec_time *= 0.00004897
        lambda_exec_time += 0.0000002  # request cost
        times["Workload Size (MB)"].append(int(size))
        if int(size) < 1024:
            times["Cost (USD)"].append(lambda_exec_time)
        else:
            times["Cost (USD)"].append(None)
        times["Approaches"].append("AWS Lambda + AWS Wrangler")

        pythonshell_exec_time = float(pythonshell[size]["ExecutionTime"])
        if pythonshell_exec_time <= 60.0:
            pythonshell_cost = 0.44 / 60.0
        else:
            pythonshell_cost = (0.44 * pythonshell_exec_time) / 3600.0
        times["Cost (USD)"].append(pythonshell_cost)
        times["Approaches"].append("AWS Glue + AWS Wrangler")

        pyspark_exec_time = float(pyspark[size]["ExecutionTime"])
        if pyspark_exec_time <= 600.0:
            pyspark_cost = 0.44 / 6.0
            pyspark_cost *= 2
        else:
            pyspark_cost = (0.44 * pyspark_exec_time) / 3600.0
            pyspark_cost *= 2
        times["Workload Size (MB)"].append(int(size))
        times["Cost (USD)"].append(pyspark_cost)
        times["Approaches"].append("AWS Glue + Pyspark")

    df = pd.DataFrame(times)
    plt.figure()
    sns.barplot(x="Workload Size (MB)", y="Cost (USD)", hue="Approaches", data=df)
    plt.yticks(np.arange(0, 0.16, 0.01))
    plt.savefig("docs/source/_static/report_cost.png")


def main():
    awslambda = json.load(open("metrics/lambda.json"))
    pythonshell = json.load(open("metrics/pythonshell.json"))
    pyspark = json.load(open("metrics/pyspark.json"))

    report_execution_time(awslambda, pythonshell, pyspark)
    report_total_time(awslambda, pythonshell, pyspark)
    report_cost(awslambda, pythonshell, pyspark)


if __name__ == "__main__":
    main()
