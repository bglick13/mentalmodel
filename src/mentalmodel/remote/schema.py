from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import psycopg


@dataclass(slots=True, frozen=True)
class SchemaMigration:
    name: str
    sql: str


REMOTE_RUNS_MIGRATIONS: Final[tuple[SchemaMigration, ...]] = (
    SchemaMigration(
        "remote_runs_table",
        """
        create table if not exists remote_runs (
            graph_id text not null,
            run_id text not null,
            created_at_ms bigint not null,
            completed_at_ms bigint,
            status text not null,
            success boolean,
            invocation_name text,
            project_id text,
            project_label text,
            environment_name text,
            catalog_entry_id text,
            catalog_source text,
            runtime_default_profile_name text,
            runtime_profile_names jsonb not null,
            run_schema_version integer not null,
            record_schema_version integer,
            records_indexed_at_ms bigint,
            spans_indexed_at_ms bigint,
            metrics_indexed_at_ms bigint,
            manifest_json jsonb not null,
            summary_json jsonb not null,
            artifact_prefix text not null,
            updated_at_ms bigint not null,
            primary key (graph_id, run_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_runs_records_indexed_column",
        "alter table remote_runs add column if not exists records_indexed_at_ms bigint",
    ),
    SchemaMigration(
        "remote_runs_spans_indexed_column",
        "alter table remote_runs add column if not exists spans_indexed_at_ms bigint",
    ),
    SchemaMigration(
        "remote_runs_metrics_indexed_column",
        "alter table remote_runs add column if not exists metrics_indexed_at_ms bigint",
    ),
    SchemaMigration(
        "remote_runs_created_idx",
        "create index if not exists idx_remote_runs_created_at on remote_runs (created_at_ms desc)",
    ),
    SchemaMigration(
        "remote_runs_project_idx",
        "create index if not exists idx_remote_runs_project_id on remote_runs (project_id)",
    ),
    SchemaMigration(
        "remote_runs_invocation_idx",
        "create index if not exists idx_remote_runs_invocation_name "
        "on remote_runs (invocation_name)",
    ),
    SchemaMigration(
        "remote_runs_graph_invocation_created_idx",
        "create index if not exists idx_remote_runs_graph_invocation_created_at "
        "on remote_runs (graph_id, invocation_name, created_at_ms desc)",
    ),
    SchemaMigration(
        "remote_run_records_table",
        """
        create table if not exists remote_run_records (
            graph_id text not null,
            run_id text not null,
            record_id text not null,
            sequence bigint not null,
            timestamp_ms bigint not null,
            node_id text not null,
            frame_id text not null,
            loop_node_id text null,
            iteration_index integer null,
            event_type text not null,
            payload_json jsonb not null,
            primary key (graph_id, run_id, record_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_run_spans_table",
        """
        create table if not exists remote_run_spans (
            graph_id text not null,
            run_id text not null,
            span_id text not null,
            sequence bigint not null,
            start_time_ns bigint not null,
            node_id text null,
            frame_id text null,
            loop_node_id text null,
            iteration_index integer null,
            runtime_profile text null,
            payload_json jsonb not null,
            primary key (graph_id, run_id, span_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_run_records_sequence_idx",
        "create index if not exists idx_remote_run_records_sequence "
        "on remote_run_records (graph_id, run_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_run_records_node_frame_sequence_idx",
        "create index if not exists idx_remote_run_records_node_frame_sequence "
        "on remote_run_records (graph_id, run_id, node_id, frame_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_run_records_timestamp_idx",
        "create index if not exists idx_remote_run_records_timestamp "
        "on remote_run_records (graph_id, timestamp_ms desc)",
    ),
    SchemaMigration(
        "remote_run_records_iteration_idx",
        "create index if not exists idx_remote_run_records_iteration "
        "on remote_run_records (graph_id, run_id, iteration_index)",
    ),
    SchemaMigration(
        "remote_run_spans_sequence_idx",
        "create index if not exists idx_remote_run_spans_sequence "
        "on remote_run_spans (graph_id, run_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_run_spans_node_frame_sequence_idx",
        "create index if not exists idx_remote_run_spans_node_frame_sequence "
        "on remote_run_spans (graph_id, run_id, node_id, frame_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_run_spans_iteration_idx",
        "create index if not exists idx_remote_run_spans_iteration "
        "on remote_run_spans (graph_id, run_id, iteration_index)",
    ),
    SchemaMigration(
        "remote_run_metrics_table",
        """
        create table if not exists remote_run_metrics (
            graph_id text not null,
            run_id text not null,
            metric_row_id text not null,
            node_id text not null,
            frame_id text null,
            loop_node_id text null,
            iteration_index integer null,
            path text not null,
            label text not null,
            normalized_label text not null,
            metric_node_path text not null,
            unit text not null,
            semantic_kind text not null,
            value double precision not null,
            primary key (graph_id, run_id, metric_row_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_run_metrics_iteration_idx",
        "create index if not exists idx_remote_run_metrics_iteration "
        "on remote_run_metrics (graph_id, run_id, iteration_index)",
    ),
    SchemaMigration(
        "remote_run_metrics_node_frame_iteration_idx",
        "create index if not exists idx_remote_run_metrics_node_frame_iteration "
        "on remote_run_metrics (graph_id, run_id, node_id, frame_id, iteration_index)",
    ),
    SchemaMigration(
        "remote_run_metrics_path_prefix_idx",
        "create index if not exists idx_remote_run_metrics_path_prefix "
        "on remote_run_metrics (graph_id, run_id, path text_pattern_ops)",
    ),
    SchemaMigration(
        "remote_run_metrics_label_prefix_idx",
        "create index if not exists idx_remote_run_metrics_label_prefix "
        "on remote_run_metrics (graph_id, run_id, label text_pattern_ops)",
    ),
    SchemaMigration(
        "remote_run_metrics_normalized_label_prefix_idx",
        "create index if not exists idx_remote_run_metrics_normalized_label_prefix "
        "on remote_run_metrics (graph_id, run_id, normalized_label text_pattern_ops)",
    ),
    SchemaMigration(
        "remote_run_metrics_metric_node_path_prefix_idx",
        "create index if not exists idx_remote_run_metrics_metric_node_path_prefix "
        "on remote_run_metrics (graph_id, run_id, metric_node_path text_pattern_ops)",
    ),
)


REMOTE_PROJECT_MIGRATIONS: Final[tuple[SchemaMigration, ...]] = (
    SchemaMigration(
        "remote_projects_table",
        """
        create table if not exists remote_projects (
            project_id text primary key,
            label text not null,
            description text not null,
            default_environment text,
            catalog_provider text,
            default_runs_dir text,
            default_verify_spec text,
            linked_at_ms bigint not null,
            updated_at_ms bigint not null,
            catalog_snapshot_json jsonb,
            catalog_entry_count integer not null default 0,
            catalog_published_at_ms bigint,
            catalog_version integer,
            last_completed_run_upload_at_ms bigint,
            last_completed_run_graph_id text,
            last_completed_run_id text,
            last_completed_run_invocation_name text
        )
        """,
    ),
    SchemaMigration(
        "remote_projects_completed_upload_at_column",
        "alter table remote_projects add column if not exists "
        "last_completed_run_upload_at_ms bigint",
    ),
    SchemaMigration(
        "remote_projects_completed_graph_column",
        "alter table remote_projects add column if not exists last_completed_run_graph_id text",
    ),
    SchemaMigration(
        "remote_projects_completed_run_column",
        "alter table remote_projects add column if not exists last_completed_run_id text",
    ),
    SchemaMigration(
        "remote_projects_completed_invocation_column",
        "alter table remote_projects add column if not exists "
        "last_completed_run_invocation_name text",
    ),
    SchemaMigration(
        "remote_projects_updated_idx",
        "create index if not exists idx_remote_projects_updated_at "
        "on remote_projects (updated_at_ms desc)",
    ),
)


REMOTE_LIVE_MIGRATIONS: Final[tuple[SchemaMigration, ...]] = (
    SchemaMigration(
        "remote_live_sessions_table",
        """
        create table if not exists remote_live_sessions (
            graph_id text not null,
            run_id text not null,
            project_id text null,
            invocation_name text null,
            environment_name text null,
            catalog_entry_id text null,
            catalog_source text null,
            runtime_default_profile_name text null,
            runtime_profile_names jsonb not null default '[]'::jsonb,
            started_at_ms bigint not null,
            updated_at_ms bigint not null,
            finished_at_ms bigint null,
            status text not null,
            error text null,
            graph_json jsonb not null,
            analysis_json jsonb not null,
            bundle_committed_at_ms bigint null,
            primary key (graph_id, run_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_live_records_table",
        """
        create table if not exists remote_live_records (
            graph_id text not null,
            run_id text not null,
            record_id text not null,
            sequence bigint not null,
            timestamp_ms bigint not null,
            node_id text not null,
            frame_id text not null,
            loop_node_id text null,
            iteration_index integer null,
            event_type text not null default '',
            payload_json jsonb not null,
            primary key (graph_id, run_id, record_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_live_metrics_table",
        """
        create table if not exists remote_live_metrics (
            graph_id text not null,
            run_id text not null,
            metric_row_id text not null,
            node_id text not null,
            frame_id text null,
            loop_node_id text null,
            iteration_index integer null,
            path text not null,
            label text not null,
            normalized_label text not null,
            metric_node_path text not null,
            unit text not null,
            semantic_kind text not null,
            value double precision not null,
            primary key (graph_id, run_id, metric_row_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_live_spans_table",
        """
        create table if not exists remote_live_spans (
            graph_id text not null,
            run_id text not null,
            span_id text not null,
            sequence bigint not null,
            start_time_ns bigint not null,
            node_id text null,
            frame_id text null,
            loop_node_id text null,
            iteration_index integer null,
            runtime_profile text null,
            payload_json jsonb not null,
            primary key (graph_id, run_id, span_id)
        )
        """,
    ),
    SchemaMigration(
        "remote_live_records_loop_node_column",
        "alter table remote_live_records add column if not exists loop_node_id text null",
    ),
    SchemaMigration(
        "remote_live_records_iteration_column",
        "alter table remote_live_records add column if not exists iteration_index integer null",
    ),
    SchemaMigration(
        "remote_live_records_event_type_column",
        (
            "alter table remote_live_records add column if not exists "
            "event_type text not null default ''"
        ),
    ),
    SchemaMigration(
        "remote_live_spans_frame_column",
        "alter table remote_live_spans add column if not exists frame_id text null",
    ),
    SchemaMigration(
        "remote_live_spans_loop_node_column",
        "alter table remote_live_spans add column if not exists loop_node_id text null",
    ),
    SchemaMigration(
        "remote_live_spans_iteration_column",
        "alter table remote_live_spans add column if not exists iteration_index integer null",
    ),
    SchemaMigration(
        "remote_live_spans_runtime_profile_column",
        "alter table remote_live_spans add column if not exists runtime_profile text null",
    ),
    SchemaMigration(
        "remote_live_sessions_lookup_idx",
        "create index if not exists idx_remote_live_sessions_lookup "
        "on remote_live_sessions (graph_id, invocation_name, started_at_ms desc)",
    ),
    SchemaMigration(
        "remote_live_records_sequence_idx",
        "create index if not exists idx_remote_live_records_sequence "
        "on remote_live_records (graph_id, run_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_live_records_node_frame_sequence_idx",
        "create index if not exists idx_remote_live_records_node_frame_sequence "
        "on remote_live_records (graph_id, run_id, node_id, frame_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_live_records_event_type_idx",
        "create index if not exists idx_remote_live_records_event_type "
        "on remote_live_records (graph_id, run_id, event_type, sequence desc)",
    ),
    SchemaMigration(
        "remote_live_spans_sequence_idx",
        "create index if not exists idx_remote_live_spans_sequence "
        "on remote_live_spans (graph_id, run_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_live_spans_node_frame_sequence_idx",
        "create index if not exists idx_remote_live_spans_node_frame_sequence "
        "on remote_live_spans (graph_id, run_id, node_id, frame_id, sequence desc)",
    ),
    SchemaMigration(
        "remote_live_metrics_iteration_idx",
        "create index if not exists idx_remote_live_metrics_iteration "
        "on remote_live_metrics (graph_id, run_id, iteration_index)",
    ),
    SchemaMigration(
        "remote_live_metrics_node_frame_iteration_idx",
        "create index if not exists idx_remote_live_metrics_node_frame_iteration "
        "on remote_live_metrics (graph_id, run_id, node_id, frame_id, iteration_index)",
    ),
    SchemaMigration(
        "remote_live_metrics_path_prefix_idx",
        "create index if not exists idx_remote_live_metrics_path_prefix "
        "on remote_live_metrics (graph_id, run_id, path text_pattern_ops)",
    ),
    SchemaMigration(
        "remote_live_metrics_label_prefix_idx",
        "create index if not exists idx_remote_live_metrics_label_prefix "
        "on remote_live_metrics (graph_id, run_id, label text_pattern_ops)",
    ),
    SchemaMigration(
        "remote_live_metrics_normalized_label_prefix_idx",
        "create index if not exists idx_remote_live_metrics_normalized_label_prefix "
        "on remote_live_metrics (graph_id, run_id, normalized_label text_pattern_ops)",
    ),
    SchemaMigration(
        "remote_live_metrics_metric_node_path_prefix_idx",
        "create index if not exists idx_remote_live_metrics_metric_node_path_prefix "
        "on remote_live_metrics (graph_id, run_id, metric_node_path text_pattern_ops)",
    ),
)


REMOTE_EVENT_MIGRATIONS: Final[tuple[SchemaMigration, ...]] = (
    SchemaMigration(
        "remote_operation_events_table",
        """
        create table if not exists remote_operation_events (
            event_id text primary key,
            occurred_at_ms bigint not null,
            kind text not null,
            status text not null,
            project_id text,
            graph_id text,
            run_id text,
            invocation_name text,
            error_category text,
            error_message text,
            metadata_json jsonb not null default '{}'::jsonb
        )
        """,
    ),
    SchemaMigration(
        "remote_operation_events_project_idx",
        "create index if not exists remote_operation_events_project_idx "
        "on remote_operation_events (project_id, occurred_at_ms desc)",
    ),
    SchemaMigration(
        "remote_operation_events_run_idx",
        "create index if not exists remote_operation_events_run_idx "
        "on remote_operation_events (graph_id, run_id, occurred_at_ms desc)",
    ),
)


def apply_schema_migrations(
    conn: psycopg.Connection[object],
    migrations: tuple[SchemaMigration, ...],
) -> None:
    for migration in migrations:
        conn.execute(migration.sql)
