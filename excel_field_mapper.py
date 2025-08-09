# pip install rapidfuzz
from rapidfuzz import fuzz
import re
from typing import Any, Dict, List, Tuple, Iterable

# ---- Нормализация и алиасы ----

UNIT_PAREN_RE = re.compile(r"\[[^\]]*\]|\([^)]*\)")
TEMPLATE_RE = re.compile(r"\$\{[^}]*\}")

ALIASES = {
    # каноники согласно типичным Excel-заголовкам
    "code": {"id", "key", "keye", "keye code", "keye key", "thread code"},
    "description": {"desc", "name", "label"},
    "supplier": {"vendor", "provider"},
    "country": {"made in", "origin"},
    "yes no": {"yes/no", "y/n"},
    "moq": {"minimum order qty", "min order qty"},
    "uom": {"unit", "unit of measure"},
    "lt": {"lead time"},
    # добавляй по мере надобности
}

STOPWORDS = {
    "old", "the", "a", "an", "of", "for", "type", "indicator", "description",
    "code", "id", "key", "keye", "material", "component", "catalogue", "catalog",
    "supplier", "vendor", "galvanic", "treatment", "plating"
}

def _canonical_token(tok: str) -> str:
    t = tok
    # приведение некоторых паттернов к канону
    if t in {"yes/no", "yesno"}:
        return "yes no"
    return t

def _alias_boost(query_tokens: List[str], cand_tokens: List[str]) -> int:
    """
    Если наборы токенов соответствуют одному канону через ALIASES — дадим бонус.
    """
    def to_canon_set(tokens: Iterable[str]) -> set:
        out = set()
        for t in tokens:
            tcanon = _canonical_token(t)
            matched = False
            for canon, variants in ALIASES.items():
                if tcanon == canon or tcanon in variants:
                    out.add(canon)
                    matched = True
                    break
            if not matched:
                out.add(tcanon)
        return out

    q = to_canon_set(query_tokens)
    c = to_canon_set(cand_tokens)
    inter = len(q & c)
    # Небольшой бонус за пересечение канонов
    return 5 * inter

def normalize_header(s: str) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = TEMPLATE_RE.sub(" ", s)               # убираем ${...}
    s = UNIT_PAREN_RE.sub(" ", s)             # убираем [μ], (weeks) и т.п.
    s = re.sub(r"[/_:-]+", " ", s)            # разделители → пробел
    s = re.sub(r"\s+", " ", s).strip()
    s = s.lower()
    # разворачиваем общие сокращения
    s = s.replace("qty", "quantity")
    s = s.replace("uom", "uom")
    s = s.replace("lt", "lt")  # позже в алиасах трактуем как lead time
    # выкидываем стоп-слова на границе, но аккуратно: сначала токенизация
    toks = [t for t in re.split(r"\s+", s) if t and t not in STOPWORDS]
    toks = [_canonical_token(t) for t in toks]
    return " ".join(toks)

def tokenize(s: str) -> List[str]:
    return [t for t in re.split(r"\s+", s) if t]


# ---- Извлечение сопоставлений модель → Excel ----

SRC_KEYS = {"src", "idSrc", "codeSrc"}  # сюда добавим при необходимости

def extract_mappings(cfg: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Возвращает список (model_path, excel_source_name).
    model_path — точка/индексированный путь до модельного поля.
    """
    out: List[Tuple[str, str]] = []

    def add(model_path: str, excel_name: str):
        if excel_name:
            out.append((model_path, excel_name))

    def visit_transformer(node: Dict[str, Any], path: str):
        if not isinstance(node, dict):
            return

        tname = node.get("name")
        conf = node.get("config", {})

        # Если есть прямые источники
        for k in SRC_KEYS:
            if k in conf:
                add(path, conf[k])

        # Специфика известных трансформеров
        if tname == "child":
            # обходим вложенные поля
            fields = conf.get("fields", [])
            for f in fields:
                fname = f.get("name")
                ftr = f.get("transformer")
                if fname and ftr:
                    visit_transformer(ftr, f"{path}.{fname}" if path else fname)
        elif tname == "list":
            items = conf.get("items", [])
            for idx, item in enumerate(items):
                itr = item.get("transformer")
                if itr:
                    visit_transformer(itr, f"{path}[{idx}]")
        elif tname == "library":
            # у library источники уже собраны через SRC_KEYS, но на всякий случай
            pass
        elif tname == "copy":
            # у copy источник уже собран через SRC_KEYS
            pass
        else:
            # неизвестный трансформер — смотрим рекурсивно в config
            _recurse_unknown(conf, path)

    def _recurse_unknown(obj: Any, path: str):
        if isinstance(obj, dict):
            # собрать возможные источники и вложенные структуры
            for k, v in obj.items():
                if k in SRC_KEYS and isinstance(v, str):
                    add(path, v)
                else:
                    _recurse_unknown(v, path)
        elif isinstance(obj, list):
            for idx, it in enumerate(obj):
                _recurse_unknown(it, f"{path}[{idx}]")

    # корневой обход по полям
    for f in cfg.get("fields", []):
        name = f.get("name")
        tr = f.get("transformer")
        if not name or not tr:
            continue
        visit_transformer(tr, name)

    return out


# ---- Индексация и поиск ----

class ModelIndex:
    def __init__(self, mappings: List[Tuple[str, str]]):
        """
        mappings: [(model_path, excel_source), ...]
        """
        self.mappings = mappings
        # Словарь excel_source_norm -> [(excel_source_original, model_path)]
        self.by_excel_norm: Dict[str, List[Tuple[str, str]]] = {}
        self.all_excel_sources: List[str] = []

        for mp, ex in mappings:
            ex_norm = normalize_header(ex)
            self.by_excel_norm.setdefault(ex_norm, []).append((ex, mp))
            self.all_excel_sources.append(ex)

        # Уникальные нормализованные кандидаты
        self.unique_norm_candidates = list(self.by_excel_norm.keys())

    def match(self, excel_header: str, top_k: int = 5) -> List[Dict[str, Any]]:
        q_raw = excel_header
        q_norm = normalize_header(q_raw)
        q_tokens = tokenize(q_norm)

        candidates: List[Dict[str, Any]] = []

        for c_norm in self.unique_norm_candidates:
            c_tokens = tokenize(c_norm)

            # базовый скор
            base = fuzz.token_set_ratio(q_norm, c_norm)

            # бонусы
            bonus = 0
            if q_norm and c_norm and (q_norm in c_norm or c_norm in q_norm):
                bonus += 5
            bonus += _alias_boost(q_tokens, c_tokens)

            score = min(100, base + bonus)

            # разворачиваем в конкретные пары (excel_original, model_path)
            for (excel_orig, model_path) in self.by_excel_norm[c_norm]:
                candidates.append({
                    "model_path": model_path,
                    "excel_source": excel_orig,
                    "score": score,
                    "reasons": {
                        "base": base,
                        "alias_bonus": bonus
                    }
                })

        # сортируем по убыванию балла, а при равенстве — по длине пути (чуть предпочтём более “узкие” поля)
        candidates.sort(key=lambda x: (x["score"], -len(x["model_path"])), reverse=True)
        return candidates[:top_k]


# ---- Удобная обёртка для использования ----

def build_index_from_config(cfg: Dict[str, Any]) -> ModelIndex:
    mappings = extract_mappings(cfg)
    return ModelIndex(mappings)


# ---- Пример использования ----
# cfg = {...}  # твой JSON
# index = build_index_from_config(cfg)
# results = index.match("OLD Base Material KEYE Key", top_k=8)
# for r in results:
#     print(r)
