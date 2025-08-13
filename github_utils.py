from typing import List
from github import Github
import os
import json

from github.Issue import Issue

from app_types import GithubIssue
from logger import get_logger

from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString
import pandas as pd

logger = get_logger()


def _clean_text(text: str) -> str:
    """
    Cleans the input text by removing HTML tags and unnecessary whitespace.
    """
    return "" if not text else BeautifulSoup(text, "html.parser").get_text().strip()

def _html_with_tables_to_markdown_text(html: str) -> str:
    """
    Заменяет все <table> на markdown-таблицы (pandas),
    остальной HTML очищает от тегов, сохраняя текст и базовые переносы строк.
    """
    logger.debug(f"Converting HTML to Markdown text. Input length: {len(html)}")
    soup = BeautifulSoup(html, "lxml")
    if not soup.find("table"):
        logger.debug("No <table> tags found in HTML. Cleaning text only.")
        return _clean_text(html)

    logger.debug("<table> tags found. Processing tables to Markdown.")

    # 1) Для каждой таблицы – сгенерировать Markdown и подставить плейсхолдер
    placeholders = []
    for i, table in enumerate(soup.find_all("table")):
        if not isinstance(table, Tag):
            continue

        # pandas парсит таблицу в DataFrame (берем первую, если их несколько в <table>)
        dfs = pd.read_html(str(table))
        if not dfs:
            continue
        df = dfs[0]

        # Заголовок (если есть <caption>) вынесем строкой перед таблицей
        caption = table.find("caption")
        caption_text = caption.get_text(" ", strip=True) if caption else None

        md_table = df.to_markdown(index=False)  # требует 'tabulate'
        md_block = (f"{caption_text}\n\n{md_table}" if caption_text else md_table)

        ph = f"[[[MD_TABLE_{i}]]]"
        placeholders.append((ph, md_block))
        table.replace_with(NavigableString(ph))

    # 2) Теперь из остального HTML убираем теги → получаем «чистый» текст
    # \n между блоками, чтобы не склеивать абзацы
    plain_text_with_ph = soup.get_text("\n")

    # 3) Вернём Markdown-таблицы на места плейсхолдеров
    out = plain_text_with_ph
    for ph, md in placeholders:
        # Оборачиваем таблицу пустыми строками, чтобы Markdown корректно «схватился»
        out = out.replace(ph, f"\n{md}\n")

    # Нормализуем лишние пустые строки (аккуратно, без агрессии)
    out = "\n".join(line.rstrip() for line in out.splitlines())
    while "\n\n\n" in out:
        out = out.replace("\n\n\n", "\n\n")

    return out


def to_github_issue(issue: Issue) -> GithubIssue:
    comments = "\n".join(
        [_html_with_tables_to_markdown_text(x.raw_data["body"]) for x in issue.get_comments()]
    )
    body = _html_with_tables_to_markdown_text(issue.body or "")
    comments_txt = "\n\nComments:\n" + comments if comments else ""

    return {
        "number": int(issue.number),
        "title": str(issue.title or ""),
        "body": body + comments_txt,
    }


def get_tasks_repo():
    g = Github(os.getenv("GITHUB_TOKEN"))
    repo = next(
        (
            repo
            for repo in g.get_user().get_repos()
            if repo.name == "surge-tasks-reports"
        ),
        None,
    )
    if not repo:
        raise LookupError("Repository 'surge-tasks-reports' not found.")
    return repo


def get_issues() -> List[GithubIssue]:
    issues = []
    # if os.path.exists("issues.json"):
    #     with open("issues.json", "r") as f:
    #         issues = json.load(f)
    # else:
    repo = get_tasks_repo()
    xs = repo.get_issues(state="open", labels=["ams"])
    for issue in xs:
        issues.append(issue)

    return [to_github_issue(issue) for issue in issues[2:5]]


def get_issue(number: int) -> GithubIssue:
    # logger.info("Looking for 'surge-tasks-reports' repository")
    repo = get_tasks_repo()
    # logger.info(f"Found repository: {repo.name}")
    logger.info(f"Retrieving issue #{number} from repository")
    issue = repo.get_issue(number)
    # logger.info(f"```json\n{json.dumps(issue.raw_data, indent=2)}\n```")
    x = to_github_issue(issue)
    logger.info(f"Issue fetched:\n```json\n{json.dumps(x, indent=2)}\n```")
    return x
