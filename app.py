import os
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage
import plotly.express as px
import json


# Load environment variables
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
os.environ["GOOGLE_API_KEY"] = api_key

# Initialize the model (LLaMA3 on Groq)
model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")

# Streamlit page setup
st.set_page_config(page_title="ADF Logs Viewer", layout="wide")
st.title("🔍 Azure Data Factory Toolkit")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📁 ADF CSV Analyzer", "📊 Dashboard", "ℹ️ About", "View Active Failures"])

# -------------------------------- TAB 1: CSV Analyzer -------------------------------- #
with tab1:
    st.header("📁 ADF CSV Analyzer")

    uploaded_file = st.file_uploader("Upload your ADF logs CSV file", type=["csv"])

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        # Checkbox to filter only failed logs
        show_failed_only = st.checkbox("🔴 Show Only Failed Pipelines")

        if show_failed_only:
            if 'Status' in df.columns:
                failed_df = df[df['Status'].str.lower() == 'failed']
                filtered_df = failed_df
            else:
                st.warning("No 'Status' column found.")
                filtered_df = df
        else:
            filtered_df = df

        # Display logs
        st.subheader("📋 Pipeline Execution Logs")
        st.dataframe(filtered_df, use_container_width=True)

        # Only show "Ask AI" if there are failures
        if show_failed_only and not failed_df.empty:
            st.subheader("🤖 Ask AI to Debug a Failed Pipeline")

            # Create a key like "pipeline4 - 6/10/2025, 5:03:19 PM"
            failed_df['PipelineKey'] = failed_df['Pipeline name'] + " - " + failed_df['Run start']
            pipeline_keys = failed_df['PipelineKey'].tolist()

            selected_pipeline_key = st.selectbox("Select a failed pipeline", pipeline_keys)
            selected_row = failed_df[failed_df['PipelineKey'] == selected_pipeline_key]

            if not selected_row.empty:
                error_text = selected_row['Error'].values[0]

                if st.button("🧠 Ask AI to Analyze Error"):
                    if not error_text or str(error_text).strip() == "":
                        st.info("No error message available for the selected pipeline.")
                    else:
                        messages = [
                            SystemMessage(content="You are an expert Azure Data Factory and Databricks pipeline engineer. Help diagnose pipeline failures and suggest fixes."),
                            HumanMessage(content=f"This is the error log for pipeline '{selected_pipeline_key}':\n\n{error_text}\n\nPlease analyze and suggest what went wrong and how to fix it.")
                        ]

                        with st.spinner("Thinking..."):
                            response = model.invoke(messages)

                        st.subheader("🛠️ AI Suggestions")
                        st.markdown(response.content)

    else:
        st.info("Please upload a CSV file to get started.")

# -------------------------------- TAB 2: Dashboard -------------------------------- #
with tab2:
    st.header("📊 Pipeline Dashboard")

    if uploaded_file is None:
        st.info("Please upload a CSV file in the 'ADF CSV Analyzer' tab to view dashboard.")
    else:
        # Use the same df loaded earlier
        if 'Status' not in df.columns or 'Pipeline name' not in df.columns:
            st.warning("Required columns (like 'Status' and 'Pipeline name') not found in the CSV.")
        else:
            # KPIs
            st.subheader("📌 Key Metrics")

            status_counts = df['Status'].str.lower().value_counts()

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("✅ Succeeded", int(status_counts.get('succeeded', 0)))
            col2.metric("❌ Failed", int(status_counts.get('failed', 0)))
            col3.metric("⏳ In Progress", int(status_counts.get('inprogress', 0)))
            col4.metric("⏱️ Queued", int(status_counts.get('queued', 0)))

            # Visualization 1 + 3: Status Pie Chart + Top Failing Pipelines
            if 'Status' in df.columns and 'Pipeline name' in df.columns:
                st.subheader("📊 Pipeline Run Summary")

                col1, col2 = st.columns(2)

                with col1:
                    pie_df = df['Status'].value_counts().reset_index()
                    pie_df.columns = ['Status', 'Count']
                    fig_pie = px.pie(
                        pie_df,
                        names='Status',
                        values='Count',
                        title="Pipeline Run Status"
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)

                with col2:
                    failed_counts = (
                        df[df['Status'].str.lower() == 'failed']['Pipeline name']
                        .value_counts()
                        .nlargest(5)
                        .reset_index()
                    )
                    failed_counts.columns = ['Pipeline name', 'Failure Count']
                    fig_bar = px.bar(
                        failed_counts,
                        x='Pipeline name',
                        y='Failure Count',
                        title='Top 5 Failing Pipelines',
                        text='Failure Count'
                    )
                    fig_bar.update_layout(xaxis={'categoryorder': 'total descending'})
                    st.plotly_chart(fig_bar, use_container_width=True)

            # Visualization 2: Pipeline Execution Trend
            if 'Run start' in df.columns:
                df['Run start'] = pd.to_datetime(df['Run start'], errors='coerce')
                df['Date'] = df['Run start'].dt.date
                daily_runs = df.groupby('Date').size().reset_index(name='Run Count')

                st.subheader("📈 Pipeline Runs Over Time (Line Chart)")
                fig = px.line(
                    daily_runs,
                    x='Date',
                    y='Run Count',
                    markers=True,
                    title="Daily Pipeline Runs",
                    labels={'Date': 'Date', 'Run Count': 'Number of Runs'}
                )
                fig.update_layout(xaxis=dict(type='category'))
                st.plotly_chart(fig, use_container_width=True)

# -------------------------------- TAB 3: About -------------------------------- #
with tab3:
    st.header("ℹ️ About This App")
    st.markdown("""
    **ADF Logs Viewer** is a tool built using **Streamlit**, **Groq LLaMA3**, and **LangChain**.

    🔍 It helps you:
    - Upload and filter Azure Data Factory logs.
    - Debug failed pipelines with the help of AI.
    - Explore dashboards (coming soon).

    """)

# --------------------------------- TAB 4: Active Failures -------------------------#
with tab4:
    st.header("Active Failures")