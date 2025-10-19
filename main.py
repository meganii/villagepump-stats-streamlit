import streamlit as st
import duckdb
import urllib.request
import os
import pandas as pd

DATA_URL = "https://github.com/meganii/sandbox-github-actions-scheduler/releases/latest/download/pages.parquet"

@st.cache_resource
def init_duckdb():
    if not os.path.exists("pages.parquet"):
        urllib.request.urlretrieve(DATA_URL, "pages.parquet")

    con = duckdb.connect('app.duckdb')
    sql = """
        -- 一時テーブルに展開
        CREATE TEMP TABLE expanded_lines AS
        SELECT 
            t.id              AS page_id,
            t.title,
            t.created,
            t.updated,
            l.id              AS line_id,
            u.ord             AS line_no,
            l.created         AS line_created,
            l.updated         AS line_updated,
            l.text,
            l.userId          AS line_userId
        FROM read_parquet('pages.parquet') t
        CROSS JOIN UNNEST(t.lines) WITH ORDINALITY AS u(l, ord);
    """
    con.execute(sql)
    return con

def main():
    st.title("井戸端統計情報")

    con = init_duckdb()

    # 最新更新日時
    latest_updated = con.execute("SELECT MAX(updated) AS latest_update FROM expanded_lines").df()
    latest_updated['latest_update'] = pd.to_datetime(latest_updated['latest_update'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
    st.subheader("最終更新日時")
    st.dataframe(latest_updated)

    # 総ページ数
    st.subheader("総ページ数")
    count = con.execute("SELECT COUNT(DISTINCT page_id) AS page_count FROM expanded_lines").df()
    st.dataframe(count)

    # データプレビュー
    df = con.execute("SELECT * FROM expanded_lines WHERE title = '井戸端' ORDER BY line_no;").df()
    st.subheader("データプレビュー")
    st.dataframe(df)

    project_name = 'villagepump'
    # textを連結して、ページごとの文字数をカウント
    st.subheader("文字数が多いページトップ500")
    char_count = con.execute(f"""
        SELECT 'https://scrapbox.io/{project_name}/' || title AS url
            ,SUM(LENGTH(text)) AS total_char_count
            ,string_agg("text", '\n' ORDER BY line_no) AS block_text,
        FROM expanded_lines
        GROUP BY title
        ORDER BY total_char_count DESC
        LIMIT 100;                     
    """).df()
    st.dataframe(
        char_count,
        column_config={
            "url": st.column_config.LinkColumn(
                "URL",
                help="The URL of the page",
                display_text=rf"https://scrapbox.io/{project_name}/(.*?)$",
            )
        },
        hide_index=True,
    )

    # 作成日ごとのページ数
    pages_df = con.execute("""
    SELECT page_id, title, MIN(created) AS created
    FROM expanded_lines
    GROUP BY page_id, title
    """).df()
    pages_df['created'] = pd.to_datetime(pages_df['created'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
    pages_df['created_month'] = pages_df['created'].dt.to_period('M').astype(str)
    created_counts = pages_df.groupby('created_month').size()
    created_counts.index.name = 'month'
    created_counts_df = created_counts.reset_index(name='count').set_index('month')
    st.subheader("作成月ごとのページ数")
    st.bar_chart(created_counts_df)
    
    # 累計ページ数の推移
    created_counts_cumsum = created_counts.cumsum()
    created_counts_cumsum_df = created_counts_cumsum.reset_index(name='cumulative_count').set_index('month')
    st.subheader("累計ページ数の推移")
    st.line_chart(created_counts_cumsum_df)

    # 井戸端の底
    st.subheader("井戸端の底（更新日順）500件")
    stale_pages = con.execute(f"""
        SELECT 'https://scrapbox.io/{project_name}/' || title AS url
            ,created
            ,updated
            ,string_agg("text", '\n' ORDER BY line_no) AS block_text
        FROM expanded_lines
        GROUP BY title, created, updated
        ORDER BY updated ASC
        LIMIT 100;
    """).df()
    stale_pages["created"] = pd.to_datetime(stale_pages["created"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
    stale_pages["updated"] = pd.to_datetime(stale_pages["updated"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
    st.dataframe(
        stale_pages,
        column_config={
            "url": st.column_config.LinkColumn(
                "URL",
                help="The URL of the page",
                display_text=rf"https://scrapbox.io/{project_name}/(.*?)$",
            )
        },
        hide_index=True,
    )


if __name__ == "__main__":
    main()
