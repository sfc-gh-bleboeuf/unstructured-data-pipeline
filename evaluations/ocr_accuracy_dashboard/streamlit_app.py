"""
OCR Accuracy Evaluation Dashboard
================================
Visualizes OCR accuracy and completeness scores from the extraction_evl procedure.
This dashboard displays results from the OCR_ACCURACY table.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt

# Page config
st.set_page_config(
    page_title="OCR Accuracy Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        border-radius: 12px;
        padding: 20px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 10px 0;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .score-excellent { color: #00d26a; }
    .score-good { color: #92d050; }
    .score-average { color: #ffc000; }
    .score-poor { color: #ff6b6b; }
    .header-gradient {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.5rem;
        font-weight: 800;
    }
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# Get Snowflake session
session = get_active_session()

# Header
st.markdown('<h1 class="header-gradient">📊 OCR Accuracy Evaluation Dashboard</h1>', unsafe_allow_html=True)
st.markdown("*LLM-as-a-Judge evaluation results for document parsing quality*")
st.divider()

# Check if OCR_ACCURACY table exists and has data
try:
    check_query = "SELECT COUNT(*) as cnt FROM DOCUMENT_DB.S3_DOCUMENTS.OCR_ACCURACY"
    count_result = session.sql(check_query).collect()
    total_records = count_result[0]['CNT'] if count_result else 0
    
    if total_records == 0:
        st.warning("⚠️ No evaluation data found in OCR_ACCURACY table.")
        st.info("""
        **To generate evaluation data, run the extraction_evl procedure:**
        ```sql
        CALL extraction_evl(
            'document_db.s3_documents.parsed_documents',
            'OCR_EVL',
            'V1'
        );
        ```
        """)
        st.stop()
        
except Exception as e:
    st.error(f"❌ Error accessing OCR_ACCURACY table: {str(e)}")
    st.info("""
    **The OCR_ACCURACY table may not exist. Run the extraction_evl procedure first:**
    ```sql
    CALL extraction_evl(
        'document_db.s3_documents.parsed_documents',
        'OCR_EVL',
        'V1'
    );
    ```
    """)
    st.stop()

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Get available runs
runs_query = """
SELECT DISTINCT run_name, app_name, app_version, 
       MAX(evaluation_timestamp) as last_eval,
       COUNT(*) as doc_count
FROM DOCUMENT_DB.S3_DOCUMENTS.OCR_ACCURACY
GROUP BY run_name, app_name, app_version
ORDER BY last_eval DESC
"""
runs_df = session.sql(runs_query).to_pandas()

if len(runs_df) > 0:
    # Run selector
    run_options = runs_df['RUN_NAME'].tolist()
    selected_run = st.sidebar.selectbox(
        "📁 Select Evaluation Run",
        options=["All Runs"] + run_options,
        index=0
    )
    
    # Score threshold filter
    min_accuracy = st.sidebar.slider(
        "Minimum Accuracy Score",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.05
    )
    
    min_completeness = st.sidebar.slider(
        "Minimum Completeness Score",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.05
    )
    
    # Build query with filters
    where_clauses = []
    if selected_run != "All Runs":
        where_clauses.append(f"run_name = '{selected_run}'")
    if min_accuracy > 0:
        where_clauses.append(f"ocr_accuracy_score >= {min_accuracy}")
    if min_completeness > 0:
        where_clauses.append(f"ocr_completeness_score >= {min_completeness}")
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Load filtered data
    data_query = f"""
    SELECT 
        document_id,
        ocr_accuracy_score,
        ocr_accuracy_reasoning,
        ocr_completeness_score,
        ocr_completeness_reasoning,
        app_name,
        app_version,
        run_name,
        source_table,
        evaluation_timestamp,
        LEFT(ocr_text, 200) as text_preview
    FROM DOCUMENT_DB.S3_DOCUMENTS.OCR_ACCURACY
    WHERE {where_sql}
    ORDER BY evaluation_timestamp DESC
    """
    
    df = session.sql(data_query).to_pandas()
    
    if len(df) == 0:
        st.warning("No data matches the selected filters.")
        st.stop()
    
    # Summary metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    avg_accuracy = df['OCR_ACCURACY_SCORE'].mean()
    avg_completeness = df['OCR_COMPLETENESS_SCORE'].mean()
    total_docs = len(df)
    low_score_count = len(df[(df['OCR_ACCURACY_SCORE'] < 0.95) | (df['OCR_COMPLETENESS_SCORE'] < 0.95)])
    
    with col1:
        st.metric(
            label="📄 Documents Evaluated",
            value=f"{total_docs:,}",
            delta=None
        )
    
    with col2:
        accuracy_delta = "Excellent" if avg_accuracy >= 0.97 else ("Good" if avg_accuracy >= 0.90 else "Needs Review")
        st.metric(
            label="🎯 Avg Accuracy Score",
            value=f"{avg_accuracy:.1%}",
            delta=accuracy_delta,
            delta_color="normal" if avg_accuracy >= 0.90 else "inverse"
        )
    
    with col3:
        completeness_delta = "Excellent" if avg_completeness >= 0.97 else ("Good" if avg_completeness >= 0.90 else "Needs Review")
        st.metric(
            label="✅ Avg Completeness Score",
            value=f"{avg_completeness:.1%}",
            delta=completeness_delta,
            delta_color="normal" if avg_completeness >= 0.90 else "inverse"
        )
    
    with col4:
        st.metric(
            label="⚠️ Documents Below 95%",
            value=f"{low_score_count}",
            delta=f"{(low_score_count/total_docs)*100:.1f}% of total" if total_docs > 0 else "0%",
            delta_color="inverse" if low_score_count > 0 else "off"
        )
    
    st.divider()
    
    # Visualization tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Score Distribution", "📈 Score Comparison", "📋 Document Details", "🔍 Low Score Analysis"])
    
    with tab1:
        st.subheader("Score Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Accuracy histogram
            accuracy_chart = alt.Chart(df).mark_bar(
                color='#667eea',
                opacity=0.8,
                cornerRadiusTopLeft=4,
                cornerRadiusTopRight=4
            ).encode(
                x=alt.X('OCR_ACCURACY_SCORE:Q', 
                       bin=alt.Bin(maxbins=20), 
                       title='Accuracy Score'),
                y=alt.Y('count()', title='Number of Documents'),
                tooltip=['count()']
            ).properties(
                title='OCR Accuracy Score Distribution',
                height=300
            )
            st.altair_chart(accuracy_chart, use_container_width=True)
        
        with col2:
            # Completeness histogram
            completeness_chart = alt.Chart(df).mark_bar(
                color='#764ba2',
                opacity=0.8,
                cornerRadiusTopLeft=4,
                cornerRadiusTopRight=4
            ).encode(
                x=alt.X('OCR_COMPLETENESS_SCORE:Q', 
                       bin=alt.Bin(maxbins=20), 
                       title='Completeness Score'),
                y=alt.Y('count()', title='Number of Documents'),
                tooltip=['count()']
            ).properties(
                title='OCR Completeness Score Distribution',
                height=300
            )
            st.altair_chart(completeness_chart, use_container_width=True)
        
        # Score breakdown by category
        st.subheader("Score Categories")
        
        def categorize_score(score):
            if score >= 0.97:
                return "Excellent (≥97%)"
            elif score >= 0.90:
                return "Good (90-96%)"
            elif score >= 0.70:
                return "Average (70-89%)"
            else:
                return "Needs Review (<70%)"
        
        df['accuracy_category'] = df['OCR_ACCURACY_SCORE'].apply(categorize_score)
        df['completeness_category'] = df['OCR_COMPLETENESS_SCORE'].apply(categorize_score)
        
        col1, col2 = st.columns(2)
        
        with col1:
            accuracy_cats = df['accuracy_category'].value_counts().reset_index()
            accuracy_cats.columns = ['Category', 'Count']
            
            pie_chart = alt.Chart(accuracy_cats).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('Count:Q'),
                color=alt.Color('Category:N', scale=alt.Scale(
                    domain=['Excellent (≥97%)', 'Good (90-96%)', 'Average (70-89%)', 'Needs Review (<70%)'],
                    range=['#00d26a', '#92d050', '#ffc000', '#ff6b6b']
                )),
                tooltip=['Category', 'Count']
            ).properties(
                title='Accuracy Score Categories',
                height=300
            )
            st.altair_chart(pie_chart, use_container_width=True)
        
        with col2:
            completeness_cats = df['completeness_category'].value_counts().reset_index()
            completeness_cats.columns = ['Category', 'Count']
            
            pie_chart2 = alt.Chart(completeness_cats).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('Count:Q'),
                color=alt.Color('Category:N', scale=alt.Scale(
                    domain=['Excellent (≥97%)', 'Good (90-96%)', 'Average (70-89%)', 'Needs Review (<70%)'],
                    range=['#00d26a', '#92d050', '#ffc000', '#ff6b6b']
                )),
                tooltip=['Category', 'Count']
            ).properties(
                title='Completeness Score Categories',
                height=300
            )
            st.altair_chart(pie_chart2, use_container_width=True)
    
    with tab2:
        st.subheader("Accuracy vs Completeness Comparison")
        
        # Scatter plot
        scatter = alt.Chart(df).mark_circle(size=80, opacity=0.6).encode(
            x=alt.X('OCR_ACCURACY_SCORE:Q', 
                   title='Accuracy Score',
                   scale=alt.Scale(domain=[0, 1])),
            y=alt.Y('OCR_COMPLETENESS_SCORE:Q', 
                   title='Completeness Score',
                   scale=alt.Scale(domain=[0, 1])),
            color=alt.Color('RUN_NAME:N', title='Evaluation Run'),
            tooltip=['DOCUMENT_ID', 'OCR_ACCURACY_SCORE', 'OCR_COMPLETENESS_SCORE', 'RUN_NAME']
        ).properties(
            title='Accuracy vs Completeness Scatter Plot',
            height=450
        ).interactive()
        
        # Add reference lines
        hline = alt.Chart(pd.DataFrame({'y': [0.95]})).mark_rule(
            strokeDash=[5, 5], color='red', opacity=0.5
        ).encode(y='y:Q')
        
        vline = alt.Chart(pd.DataFrame({'x': [0.95]})).mark_rule(
            strokeDash=[5, 5], color='red', opacity=0.5
        ).encode(x='x:Q')
        
        st.altair_chart(scatter + hline + vline, use_container_width=True)
        st.caption("*Red dashed lines indicate the 95% threshold*")
        
        # Run comparison
        if len(runs_df) > 1:
            st.subheader("Run-by-Run Comparison")
            
            run_summary = df.groupby('RUN_NAME').agg({
                'OCR_ACCURACY_SCORE': ['mean', 'min', 'max', 'std'],
                'OCR_COMPLETENESS_SCORE': ['mean', 'min', 'max', 'std'],
                'DOCUMENT_ID': 'count'
            }).round(3)
            run_summary.columns = ['Avg Accuracy', 'Min Accuracy', 'Max Accuracy', 'Std Accuracy',
                                   'Avg Completeness', 'Min Completeness', 'Max Completeness', 'Std Completeness',
                                   'Doc Count']
            st.dataframe(run_summary, use_container_width=True)
    
    with tab3:
        st.subheader("Document-Level Details")
        
        # Search box
        search_term = st.text_input("🔍 Search by Document ID", "")
        
        display_df = df.copy()
        if search_term:
            display_df = display_df[display_df['DOCUMENT_ID'].str.contains(search_term, case=False, na=False)]
        
        # Format for display
        display_cols = ['DOCUMENT_ID', 'OCR_ACCURACY_SCORE', 'OCR_COMPLETENESS_SCORE', 
                       'RUN_NAME', 'EVALUATION_TIMESTAMP']
        
        st.dataframe(
            display_df[display_cols].style.format({
                'OCR_ACCURACY_SCORE': '{:.1%}',
                'OCR_COMPLETENESS_SCORE': '{:.1%}'
            }).background_gradient(subset=['OCR_ACCURACY_SCORE', 'OCR_COMPLETENESS_SCORE'], 
                                  cmap='RdYlGn', vmin=0, vmax=1),
            use_container_width=True,
            height=400
        )
        
        # Document detail expander
        st.subheader("📄 Document Detail View")
        selected_doc = st.selectbox(
            "Select a document to view details",
            options=display_df['DOCUMENT_ID'].tolist()
        )
        
        if selected_doc:
            doc_data = display_df[display_df['DOCUMENT_ID'] == selected_doc].iloc[0]
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**🎯 Accuracy Score**")
                st.progress(float(doc_data['OCR_ACCURACY_SCORE']))
                st.write(f"Score: {doc_data['OCR_ACCURACY_SCORE']:.1%}")
                st.markdown("**Reasoning:**")
                st.info(doc_data['OCR_ACCURACY_REASONING'] if pd.notna(doc_data['OCR_ACCURACY_REASONING']) else "No reasoning available")
            
            with col2:
                st.markdown("**✅ Completeness Score**")
                st.progress(float(doc_data['OCR_COMPLETENESS_SCORE']))
                st.write(f"Score: {doc_data['OCR_COMPLETENESS_SCORE']:.1%}")
                st.markdown("**Reasoning:**")
                st.info(doc_data['OCR_COMPLETENESS_REASONING'] if pd.notna(doc_data['OCR_COMPLETENESS_REASONING']) else "No reasoning available")
            
            st.markdown("**📝 Text Preview:**")
            st.text_area("", value=doc_data['TEXT_PREVIEW'] if pd.notna(doc_data['TEXT_PREVIEW']) else "No preview available", height=150, disabled=True)
    
    with tab4:
        st.subheader("⚠️ Documents Requiring Review")
        st.markdown("*Documents with accuracy or completeness scores below 95%*")
        
        low_score_df = df[(df['OCR_ACCURACY_SCORE'] < 0.95) | (df['OCR_COMPLETENESS_SCORE'] < 0.95)]
        
        if len(low_score_df) == 0:
            st.success("🎉 All documents have scores above 95%! Great OCR quality.")
        else:
            st.warning(f"Found {len(low_score_df)} documents with scores below 95%")
            
            # Sort by lowest score
            low_score_df = low_score_df.sort_values('OCR_ACCURACY_SCORE')
            
            for idx, row in low_score_df.head(10).iterrows():
                with st.expander(f"📄 {row['DOCUMENT_ID']} - Accuracy: {row['OCR_ACCURACY_SCORE']:.1%} | Completeness: {row['OCR_COMPLETENESS_SCORE']:.1%}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Accuracy Reasoning:**")
                        st.write(row['OCR_ACCURACY_REASONING'] if pd.notna(row['OCR_ACCURACY_REASONING']) else "No reasoning")
                    with col2:
                        st.markdown("**Completeness Reasoning:**")
                        st.write(row['OCR_COMPLETENESS_REASONING'] if pd.notna(row['OCR_COMPLETENESS_REASONING']) else "No reasoning")
                    
                    st.markdown("**Text Preview:**")
                    st.code(row['TEXT_PREVIEW'] if pd.notna(row['TEXT_PREVIEW']) else "No preview", language=None)
            
            if len(low_score_df) > 10:
                st.info(f"Showing top 10 of {len(low_score_df)} documents. Use filters to narrow down.")

else:
    st.warning("No evaluation runs found.")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.8rem;'>
    <p>OCR Accuracy Dashboard | Powered by Snowflake Cortex AI & TruLens</p>
    <p>Evaluation uses LLM-as-a-Judge (llama3.1-70b) for accuracy and completeness scoring</p>
</div>
""", unsafe_allow_html=True)

