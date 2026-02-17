from __future__ import annotations

from multiprocessing import shared_memory


def test_release_attached_shm_refs_removes_selected_handles():
    from services.python_grpc.src.vision_validation import worker

    shm1 = shared_memory.SharedMemory(create=True, size=8)
    shm2 = shared_memory.SharedMemory(create=True, size=8)

    try:
        worker._attached_shms[shm1.name] = shm1
        worker._attached_shms[shm2.name] = shm2

        worker.release_attached_shm_refs([shm1.name])

        assert shm1.name not in worker._attached_shms
        assert shm2.name in worker._attached_shms

        worker.release_attached_shm_refs([shm2.name])
        assert shm2.name not in worker._attached_shms
    finally:
        # 兼容“已 close”与“仍存在”两种状态。
        for shm in (shm1, shm2):
            try:
                shm.close()
            except Exception:
                pass
            try:
                shm.unlink()
            except Exception:
                pass


def test_release_attached_shm_refs_none_releases_all():
    from services.python_grpc.src.vision_validation import worker

    shm1 = shared_memory.SharedMemory(create=True, size=8)
    shm2 = shared_memory.SharedMemory(create=True, size=8)

    try:
        worker._attached_shms[shm1.name] = shm1
        worker._attached_shms[shm2.name] = shm2

        worker.release_attached_shm_refs(None)

        assert shm1.name not in worker._attached_shms
        assert shm2.name not in worker._attached_shms
    finally:
        for shm in (shm1, shm2):
            try:
                shm.close()
            except Exception:
                pass
            try:
                shm.unlink()
            except Exception:
                pass

