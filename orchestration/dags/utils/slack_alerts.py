"""
orchestration/dags/utils/slack_alerts.py
==========================================
Callbacks de Airflow para notificaciones de éxito/fallo vía Slack.

Se usan como on_failure_callback y on_success_callback en el DAG.

Configuración:
  - SLACK_WEBHOOK_URL: webhook URL del canal #data-engineering
  - Definir en Airflow Connections: conn_id='slack_webhook' (tipo HTTP)
    o como variable de entorno SLACK_WEBHOOK_URL.
"""

import os
from datetime import datetime


def _build_slack_message(context: dict, status: str, color: str) -> dict:
    """Construye el payload del mensaje Slack con formato enriquecido."""
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("run_id", "")
    execution_date = context.get("execution_date", datetime.utcnow())
    log_url = context.get("task_instance").log_url

    emoji = "✅" if status == "SUCCESS" else "❌"

    return {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"{emoji} Airflow — {status}"},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                            {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                            {"type": "mrkdwn", "text": f"*Run ID:*\n`{run_id}`"},
                            {
                                "type": "mrkdwn",
                                "text": f"*Fecha (UTC):*\n`{execution_date.strftime('%Y-%m-%d %H:%M')}`",
                            },
                        ],
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "Ver Logs"},
                                "url": log_url,
                            }
                        ],
                    },
                ],
            }
        ]
    }


def on_failure_callback(context: dict) -> None:
    """
    Callback invocado por Airflow cuando una task falla.
    Se registra en on_failure_callback del DAG o de tasks individuales.
    """
    import requests

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("⚠️  SLACK_WEBHOOK_URL no configurado. Omitiendo notificación.")
        return

    payload = _build_slack_message(context, status="FAILURE", color="#FF0000")

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print("✅ Notificación de fallo enviada a Slack.")
    except Exception as e:
        print(f"⚠️  Error enviando notificación a Slack: {e}")


def on_success_callback(context: dict) -> None:
    """
    Callback invocado cuando el DAG completo termina exitosamente.
    Se usa en on_success_callback del DAG (no de tasks individuales).
    """
    import requests

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    payload = _build_slack_message(context, status="SUCCESS", color="#36a64f")

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print("✅ Notificación de éxito enviada a Slack.")
    except Exception as e:
        print(f"⚠️  Error enviando notificación a Slack: {e}")
