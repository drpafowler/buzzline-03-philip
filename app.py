import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# Load the data
df = pd.read_csv('data/hourly_data_received.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Create the Streamlit app
st.title('Real-Time Dew Point Calculator')

# Add a button to refresh the data
if st.button('Refresh Data'):

    # Display the updated data
    st.dataframe(df[['timestamp', 'drybulb_c', 'wetbulb_c', 'pressure_hpa', 'dewpoint_c']].tail(10))

    # Create a line chart
    fig = go.Figure(data=[go.Scatter(x=df['timestamp'], y=df['dewpoint_c'], mode='lines+markers')])
    fig.update_layout(title='Dewpoint Over Time', xaxis_title='Date', yaxis_title='Dewpoint Temperature (C)')

    # Display the chart
    st.plotly_chart(fig)