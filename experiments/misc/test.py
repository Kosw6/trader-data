import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results.csv")

# 케이스 분류
def classify(row):
    if row['thread'] <= 4:
        return 'Stress'
    elif row['thread'] == 30:
        return 'Hikari'
    elif row['hikari'] == 8:
        return 'Thread'
    else:
        return 'Combo'

df['type'] = df.apply(classify, axis=1)

# 색 지정
colors = {
    'Hikari': 'blue',
    'Thread': 'green',
    'Combo': 'gray',
    'Stress': 'red'
}

# =========================
# nvcswch (핵심 그래프)
# =========================
plt.figure()

for t in df['type'].unique():
    subset = df[df['type'] == t]
    plt.scatter(subset['thread'], subset['pid_nvcswch'],
                label=t, color=colors[t], s=80)

plt.title("Thread vs Involuntary Context Switching")
plt.xlabel("Thread Count")
plt.ylabel("nvcswch")
plt.legend()
plt.grid()
plt.savefig("nvcswch_scatter.png")

# =========================
# cswch
# =========================
plt.figure()

for t in df['type'].unique():
    subset = df[df['type'] == t]
    plt.scatter(subset['thread'], subset['pid_cswch'],
                label=t, color=colors[t], s=80)

plt.title("Thread vs Voluntary Context Switching")
plt.xlabel("Thread Count")
plt.ylabel("cswch")
plt.legend()
plt.grid()
plt.savefig("cswch_scatter.png")

print("scatter 그래프 생성 완료")