import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# Load the data
df = pd.read_csv('data/data.csv', header=None)
df.columns = ['Datetime', 'Temperature']
df['Datetime'] = pd.to_datetime(df['Datetime'])

# Create the Streamlit app
st.title('Real-Time Temperature Monitor')

# Add a button to refresh the data
if st.button('Refresh Data'):
  # Read the updated data
  df = pd.read_csv('data/data.csv', header=None)
  df.columns = ['Datetime', 'Temperature']
  df['Datetime'] = pd.to_datetime(df['Datetime'])

  # Display the updated data
  st.dataframe(df[['Datetime', 'Temperature']].tail(10))

# Create a line chart
fig = go.Figure(data=[go.Scatter(x=df['Datetime'], y=df['Temperature'], mode='lines+markers')])

# Set the title and axis labels
fig.update_layout(title='Temperature Over Time', xaxis_title='Datetime', yaxis_title='Temperature (F)')

# Display the chart
st.plotly_chart(fig)