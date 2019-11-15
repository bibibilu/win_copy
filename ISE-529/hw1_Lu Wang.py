import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

df = pd.read_csv("C:/Users/Claire/Downloads/529/vitoria2006.csv")
q1 = df[(df.garage == 1) & (df.totalprice > 400000)]
# print(q1.shape[0])

total_price = df['totalprice']
# print(a)
res = stats.relfreq(total_price, numbins=40)
cum_freq, l_limit, bin_size, extra_p = stats.relfreq(total_price, numbins=40)
x = l_limit + np.linspace(0, bin_size*res.frequency.size, res.frequency.size)
a, b, c, d = stats.relfreq(total_price, numbins=40)

print("cumulative frequency : ", a)
print("Lower Limit : ", b)
print("bin size : ", c)
print("extra-points : ", d)

fig = plt.figure(figsize=(12, 6))
ax = fig.add_subplot(1, 1, 1)
ax.bar(x, res.frequency, width=bin_size)
ax.set_title('Relative frequency histogram')
ax.set_xlim([x.min(), x.max()])  # 设置x轴上下限
plt.grid()
sns.distplot(total_price)

plt.show()

