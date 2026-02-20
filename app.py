import streamlit as st
import pandas as pd
from urllib.parse import quote
from database import (
    init_db, search_results, get_athlete_history,
    get_filter_options, get_statistics
)

st.set_page_config(page_title="高山滑雪成绩查询系统", layout="wide")

# Initialize database on first run
init_db()

# --- Read query params for link-based navigation ---
qp = st.query_params
qp_season = qp.get("season", None)
qp_competition = qp.get("competition", None)

# If query params are set, force page to 成绩查询
if qp_season or qp_competition:
    page = "成绩查询"
    nav_index = 0
else:
    nav_index = 0

# --- Navigation ---
page = st.sidebar.radio("导航", ["成绩查询", "运动员档案", "数据管理"], index=nav_index)

# ========================================================
# Page 1: 成绩查询 (Results Search)
# ========================================================
if page == "成绩查询":
    st.title("高山滑雪成绩查询")

    # Sidebar filters
    st.sidebar.header("筛选条件")

    name_search = st.sidebar.text_input("运动员姓名", placeholder="支持中文/拼音/首字母，如：姚知涵 / yaozhihan / yzh")

    # First get seasons (always all)
    all_options = get_filter_options()

    # Determine default season from query params
    season_options = ["全部"] + all_options["seasons"]
    season_default = 0
    if qp_season and qp_season in all_options["seasons"]:
        season_default = season_options.index(qp_season)
    season = st.sidebar.selectbox("雪季", season_options, index=season_default)

    # Cascading: filter by selected season
    sel_season = season if season != "全部" else None
    options_by_season = get_filter_options(season=sel_season)

    # Determine default competition from query params
    comp_options = ["全部"] + options_by_season["competitions"]
    comp_default = 0
    if qp_competition and qp_competition in options_by_season["competitions"]:
        comp_default = comp_options.index(qp_competition)
    competition = st.sidebar.selectbox("比赛", comp_options, index=comp_default)

    sel_competition = competition if competition != "全部" else None
    options_cascaded = get_filter_options(season=sel_season, competition=sel_competition)

    discipline = st.sidebar.selectbox("项目", ["全部"] + options_cascaded["disciplines"])
    age_group = st.sidebar.selectbox("年龄组", ["全部"] + options_cascaded["age_groups"])
    gender = st.sidebar.selectbox("性别", ["全部"] + options_cascaded["genders"])

    # Clear query params after applying (so sidebar takes over)
    if qp_season or qp_competition:
        st.query_params.clear()

    # Build filters
    filters = {}
    if season != "全部":
        filters["season"] = season
    if competition != "全部":
        filters["competition"] = competition
    if discipline != "全部":
        filters["discipline"] = discipline
    if age_group != "全部":
        filters["age_group"] = age_group
    if gender != "全部":
        filters["gender"] = gender
    if name_search:
        filters["name"] = name_search

    results = search_results(filters)
    st.subheader(f"查询结果 ({len(results)} 条记录)")

    if results:
        df = pd.DataFrame(results)

        # Build link URLs for season and competition columns
        df["season_link"] = df["season"].apply(
            lambda s: f"?season={quote(str(s))}" if pd.notna(s) and s else ""
        )
        df["competition_link"] = df["competition"].apply(
            lambda c: f"?competition={quote(str(c))}" if pd.notna(c) and c else ""
        )

        # Columns for display (use link columns for season and competition)
        available = []
        col_config = {}
        col_map = [
            ("rank", "名次"),
            ("bib", "号码"),
            ("name", "姓名"),
            ("team", "代表队"),
            ("run1_time", "第一轮"),
            ("run2_time", "第二轮"),
            ("total_time", "总成绩"),
            ("time_diff", "时间差"),
            ("status", "状态"),
            ("discipline", "项目"),
            ("age_group", "年龄组"),
            ("gender", "性别"),
            ("competition_link", "比赛"),
            ("season_link", "雪季"),
        ]

        rename = {}
        for col, label in col_map:
            if col in df.columns:
                available.append(col)
                rename[col] = label

        df_display = df[available].rename(columns=rename)

        # Configure link columns
        col_config["雪季"] = st.column_config.LinkColumn(
            "雪季",
            display_text=r"\?season=(.+)"
        )
        col_config["比赛"] = st.column_config.LinkColumn(
            "比赛",
            display_text=r"\?competition=(.+)"
        )

        # Row count control
        row_options = [20, 50, 100, 200, 500]
        total_rows = len(df_display)
        default_idx = 0
        for i, opt in enumerate(row_options):
            if opt >= total_rows:
                default_idx = i
                break
        else:
            default_idx = len(row_options) - 1

        rows_per_page = st.sidebar.selectbox(
            "每页显示行数", row_options + ["全部"],
            index=default_idx if total_rows <= 500 else len(row_options)
        )

        if rows_per_page == "全部":
            st.dataframe(df_display, use_container_width=True, hide_index=True,
                         column_config=col_config,
                         height=min(35 * total_rows + 38, 800))
        else:
            rows_per_page = int(rows_per_page)
            total_pages = (total_rows + rows_per_page - 1) // rows_per_page
            if total_pages > 1:
                page_num = st.number_input(
                    f"页码 (共 {total_pages} 页)", min_value=1,
                    max_value=total_pages, value=1, step=1
                )
            else:
                page_num = 1
            start = (page_num - 1) * rows_per_page
            end = min(start + rows_per_page, total_rows)
            st.caption(f"显示第 {start+1} - {end} 条，共 {total_rows} 条")
            st.dataframe(df_display.iloc[start:end], use_container_width=True,
                         hide_index=True, column_config=col_config,
                         height=min(35 * (end - start) + 38, 800))
    else:
        st.info("没有找到匹配的记录。请调整筛选条件。")

# ========================================================
# Page 2: 运动员档案 (Athlete Profile)
# ========================================================
elif page == "运动员档案":
    st.title("运动员档案")

    athlete_name = st.text_input("输入运动员姓名", placeholder="支持中文/拼音/首字母，如：姚知涵 / yaozhihan / yzh")

    if athlete_name:
        history = get_athlete_history(athlete_name)

        if history:
            # Show actual athlete names found (may differ from input if pinyin search)
            found_names = sorted(set(r["name"] for r in history if r.get("name")))
            if len(found_names) == 1:
                st.subheader(f"{found_names[0]} — 比赛记录 ({len(history)} 条)")
            else:
                st.subheader(f"比赛记录 ({len(history)} 条)")
                st.info(f"匹配到 {len(found_names)} 名运动员：{'、'.join(found_names)}")

            # Summary stats
            col1, col2, col3 = st.columns(3)
            competitions = set(r["competition"] for r in history)
            ranks = [r["rank"] for r in history if r["rank"] is not None]
            events = set(
                (r["competition"], r["discipline"], r["age_group"])
                for r in history
            )

            col1.metric("参赛次数", len(competitions))
            col2.metric("最佳名次", min(ranks) if ranks else "N/A")
            col3.metric("比赛项目", len(events))

            # History table
            df = pd.DataFrame(history)

            # Add link columns
            df["season_link"] = df["season"].apply(
                lambda s: f"?season={quote(str(s))}" if pd.notna(s) and s else ""
            )
            df["competition_link"] = df["competition"].apply(
                lambda c: f"?competition={quote(str(c))}" if pd.notna(c) and c else ""
            )

            display_cols = [
                ("name", "姓名"),
                ("season_link", "雪季"),
                ("competition_link", "比赛"),
                ("date", "日期"),
                ("discipline", "项目"),
                ("age_group", "年龄组"),
                ("gender", "性别"),
                ("rank", "名次"),
                ("total_time", "总成绩"),
                ("time_diff", "时间差"),
                ("status", "状态"),
            ]
            available = [c for c, _ in display_cols if c in df.columns]
            rename = {c: label for c, label in display_cols if c in df.columns}
            df_display = df[available].rename(columns=rename)

            col_config = {
                "雪季": st.column_config.LinkColumn("雪季", display_text=r"\?season=(.+)"),
                "比赛": st.column_config.LinkColumn("比赛", display_text=r"\?competition=(.+)"),
            }
            st.dataframe(df_display, use_container_width=True, hide_index=True,
                         column_config=col_config)
        else:
            st.warning(f"未找到运动员 {athlete_name} 的记录。")
    else:
        st.info("请输入运动员姓名进行查询。")

# ========================================================
# Page 3: 数据管理 (Data Management)
# ========================================================
elif page == "数据管理":
    st.title("数据管理")

    # Statistics
    stats = get_statistics()
    st.subheader("数据库概览")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("比赛", stats["competitions"])
    col2.metric("赛事项目", stats["events"])
    col3.metric("成绩记录", stats["results"])
    col4.metric("运动员", stats["athletes"])

    st.divider()

    # Ingestion
    st.subheader("数据导入")
    col_a, col_b = st.columns(2)
    col_a.metric("已处理文件", stats["files_processed"])
    col_b.metric("失败文件", stats["files_failed"])

    max_files = st.number_input("最大导入文件数（0 = 全部）", min_value=0, value=0, step=10)

    if st.button("开始导入"):
        # Import here to avoid loading boto3 unless needed
        from ingestion import run_ingestion

        progress_bar = st.progress(0, text="准备导入...")
        status_text = st.empty()

        def update_progress(current, total, s3_key):
            if total > 0:
                progress_bar.progress(current / total, text=f"处理中: {s3_key}")
            status_text.text(f"进度: {current}/{total}")

        limit = max_files if max_files > 0 else None
        counts = run_ingestion(max_files=limit, progress_callback=update_progress)

        progress_bar.progress(1.0, text="导入完成！")
        st.success(
            f"导入完成！处理: {counts['processed']}，跳过: {counts['skipped']}，失败: {counts['failed']}"
        )
        st.rerun()

    # Show failed files
    st.subheader("失败文件列表")
    from database import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT s3_key, file_type, processed_at, error_message "
        "FROM processed_files WHERE status = 'failed' ORDER BY processed_at DESC"
    )
    failed = [dict(r) for r in cursor.fetchall()]
    conn.close()

    if failed:
        df_failed = pd.DataFrame(failed)
        df_failed.columns = ["文件", "类型", "处理时间", "错误信息"]
        st.dataframe(df_failed, use_container_width=True, hide_index=True)
    else:
        st.info("没有失败的文件。")
