# %%
import zarr
from helpers.do_benchmark import open_zarr
# %%
zarr_path = "r2://neurosift/scratch/qfc_benchmark/test1.zarr"
z = open_zarr(zarr_path, mode="r")
# %%
raw = z['raw']
assert isinstance(raw, zarr.Array)
A = raw[:]
filtered = z['filtered_300-6000']
assert isinstance(filtered, zarr.Array)
B = filtered[:]
# %%
m = 42000
n = 2000
ch = 16
A0 = A[m:m + n, ch]
B0 = B[m:m + n, ch]

# %%

# plot
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 4))
plt.plot(A0, label='Raw')
plt.plot(B0, label='Filtered')
plt.legend()
plt.show()

plt.figure(figsize=(12, 4))
plt.plot(B0, label='Filtered')
plt.legend()
plt.show()

# %%
# Same plots using plotly
import plotly.graph_objects as go

# First plot with both raw and filtered data
fig1 = go.Figure()
fig1.add_trace(go.Scatter(y=A0, name='Raw'))
fig1.add_trace(go.Scatter(y=B0, name='Filtered'))
fig1.update_layout(
    height=400,
    width=1200,
    showlegend=True,
    title='Raw vs Filtered Data'
)
fig1.show()

# Second plot with just filtered data
fig2 = go.Figure()
fig2.add_trace(go.Scatter(y=B0, name='Filtered'))
fig2.update_layout(
    height=400,
    width=1200,
    showlegend=True,
    title='Filtered Data'
)
fig2.show()
# %%
