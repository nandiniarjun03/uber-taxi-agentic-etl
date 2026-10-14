"""Export the LangGraph agent workflow to docs/agent_graph.mmd (+ .png when online)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.agents import (  # noqa: E402
    AgentSystem,
    OfflineLLM,
    export_graph,  # noqa: E402
)
from src.config import get_config  # noqa: E402

if __name__ == "__main__":
    cfg = get_config()
    # No Spark or Azure needed just to draw the graph.
    system = AgentSystem(cfg, llm=OfflineLLM(), runner=object(), ops=object())
    print(export_graph(system.app, str(ROOT / "docs")))
