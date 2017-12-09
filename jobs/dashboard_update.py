from google.cloud import bigquery

import plotly
import plotly.plotly as py
import plotly.graph_objs as go

import numpy as np

x = np.random.randn(500)
data = [go.Histogram(x=x)]

py.iplot(data, filename='basic histogram')
