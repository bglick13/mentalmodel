from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from mentalmodel.remote.contracts import ArtifactDescriptor, RunManifest, RemoteContractError

RUNS_DIRNAME = ".runs"


@dataclass(slots=True, frozen=True)
class UploadedArtifact:
    """One uploaded artifact body paired with its descriptor."""

    descriptor: ArtifactDescriptor
    content_base64: str

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> UploadedArtifact:
        descriptor_payload = payload.get("descriptor")
        content_base64 = payload.get("content_base64")
        if not isinstance(descriptor_payload, dict):
            raise RemoteContractError("UploadedArtifact.descriptor must be an object.")
        if not isinstance(content_base64, str):
            raise RemoteContractError("UploadedArtifact.content_base64 must be a string.")
        return cls(
            descriptor=ArtifactDescriptor.from_dict(cast(dict[str, object], descriptor_payload)),
            content_base64=content_base64,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "descriptor": self.descriptor.as_dict(),
            "content_base64": self.content_base64,
        }

    def content_bytes(self) -> bytes:
        return base64.b64decode(self.content_base64.encode("ascii"))


@dataclass(slots=True, frozen=True)
class RunBundleUpload:
    """Canonical upload payload for one completed run bundle."""

    manifest: RunManifest
    artifacts: tuple[UploadedArtifact, ...]

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RunBundleUpload:
        manifest_payload = payload.get("manifest")
        artifacts_payload = payload.get("artifacts")
        if not isinstance(manifest_payload, dict):
            raise RemoteContractError("RunBundleUpload.manifest must be an object.")
        if not isinstance(artifacts_payload, list):
            raise RemoteContractError("RunBundleUpload.artifacts must be a list.")
        return cls(
            manifest=RunManifest.from_dict(cast(dict[str, object], manifest_payload)),
            artifacts=tuple(
                UploadedArtifact.from_dict(cast(dict[str, object], item))
                for item in artifacts_payload
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "manifest": self.manifest.as_dict(),
            "artifacts": [artifact.as_dict() for artifact in self.artifacts],
        }


class FileRemoteRunStore:
    """Deterministic file-backed remote store for the first ingest slice."""

    def __init__(self, *, root_dir: Path) -> None:
        self.root_dir = root_dir

    @property
    def runs_root(self) -> Path:
        if self.root_dir.name == RUNS_DIRNAME:
            return self.root_dir
        return self.root_dir / RUNS_DIRNAME

    @property
    def manifests_root(self) -> Path:
        return self.root_dir / ".remote" / "manifests"

    def ingest(self, upload: RunBundleUpload) -> Path:
        """Persist one uploaded run bundle into the file-backed remote store."""

        missing = upload.manifest.missing_required_artifacts()
        if missing:
            raise RemoteContractError(
                f"Run upload is missing required artifacts: {', '.join(name.value for name in missing)}."
            )
        artifact_map = {
            artifact.descriptor.logical_name: artifact for artifact in upload.artifacts
        }
        manifest_names = {artifact.logical_name for artifact in upload.manifest.artifacts}
        if set(artifact_map) != manifest_names:
            raise RemoteContractError(
                "Uploaded artifacts must match the manifest artifact descriptors exactly."
            )

        run_dir = self.runs_root / upload.manifest.graph_id / upload.manifest.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        for descriptor in upload.manifest.artifacts:
            uploaded = artifact_map[descriptor.logical_name]
            content = uploaded.content_bytes()
            if descriptor.checksum_sha256 is not None:
                import hashlib

                digest = hashlib.sha256(content).hexdigest()
                if digest != descriptor.checksum_sha256:
                    raise RemoteContractError(
                        f"Checksum mismatch for artifact {descriptor.logical_name.value!r}."
                    )
            target = run_dir / descriptor.relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)

        manifest_path = (
            self.manifests_root
            / upload.manifest.graph_id
            / f"{upload.manifest.run_id}.json"
        )
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(upload.manifest.as_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return run_dir
