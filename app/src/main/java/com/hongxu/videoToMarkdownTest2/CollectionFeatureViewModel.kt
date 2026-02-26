package com.hongxu.videoToMarkdownTest2

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.ProcessLifecycleOwner
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

sealed interface ProbeUiState {
    data object Idle : ProbeUiState
    data class Loading(val input: String) : ProbeUiState
    data class Success(val result: VideoProbeResult) : ProbeUiState
    data class Error(val message: String) : ProbeUiState
}

sealed interface CollectionUiEvent {
    data class Snackbar(val message: String) : CollectionUiEvent
    data class SingleTaskSubmitted(val taskId: String, val message: String) : CollectionUiEvent
    data class BatchSubmitted(
        val collectionId: String,
        val collectionTitle: String,
        val submittedCount: Int,
        val message: String
    ) : CollectionUiEvent
}

class CollectionFeatureViewModel(
    application: Application,
    private val apiBaseUrl: String
) : AndroidViewModel(application) {

    private val repository = CollectionFeatureRepository(
        context = application.applicationContext,
        apiBaseUrl = apiBaseUrl
    )
    private val probeScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val taskCompletionNotifier = TaskCompletionNotifier(application.applicationContext)

    private val _probeState = MutableStateFlow<ProbeUiState>(ProbeUiState.Idle)
    val probeState: StateFlow<ProbeUiState> = _probeState.asStateFlow()

    private val _selectedEpisodeNos = MutableStateFlow<Set<Int>>(emptySet())
    val selectedEpisodeNos: StateFlow<Set<Int>> = _selectedEpisodeNos.asStateFlow()

    private val _collectionDetailId = MutableStateFlow<String?>(null)
    val collectionDetailId: StateFlow<String?> = _collectionDetailId.asStateFlow()

    private val _submitInProgress = MutableStateFlow(false)
    val submitInProgress: StateFlow<Boolean> = _submitInProgress.asStateFlow()

    private val _events = MutableSharedFlow<CollectionUiEvent>(extraBufferCapacity = 32)
    val events = _events.asSharedFlow()

    val collections: StateFlow<List<CollectionCardUi>> = repository.observeCollectionCards()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    val detailEpisodes: StateFlow<List<CollectionEpisodeUi>> = _collectionDetailId
        .filterNotNull()
        .flatMapLatest { collectionId -> repository.observeEpisodes(collectionId) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    val detailCollection: StateFlow<CollectionCardUi?> = combine(
        collections,
        _collectionDetailId
    ) { cards, selectedId ->
        cards.firstOrNull { it.collectionId == selectedId }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), null)

    private var realtimeCollectionJob: Job? = null
    private var probeJob: Job? = null

    init {
        refreshCollections()
    }

    fun refreshCollections() {
        viewModelScope.launch {
            runCatching {
                repository.refreshCollections()
            }.onFailure { error ->
                _events.tryEmit(
                    CollectionUiEvent.Snackbar(
                        "刷新合集失败：${error.message ?: "unknown"}"
                    )
                )
            }
        }
    }

    fun probeVideoInput(rawInput: String) {
        val normalized = rawInput.trim()
        if (normalized.isEmpty()) {
            _probeState.value = ProbeUiState.Error("请输入视频链接")
            return
        }
        probeJob?.cancel()
        _probeState.value = ProbeUiState.Loading(normalized)
        probeJob = probeScope.launch {
            runCatching {
                repository.probeVideoInfo(normalized)
            }.onSuccess { result ->
                if (!result.success) {
                    _probeState.value = ProbeUiState.Error("探测失败，请稍后重试")
                    notifyProbeFinishedIfBackground(
                        input = normalized,
                        success = false,
                        resolvedTitle = result.title,
                        detail = "empty result"
                    )
                    return@onSuccess
                }
                val defaultSelected = if (result.isCollection) {
                    result.episodes.mapNotNull { episode ->
                        episode.episodeNo.takeIf { it > 0 }
                    }.toSet()
                } else {
                    emptySet()
                }
                _selectedEpisodeNos.value = defaultSelected
                _probeState.value = ProbeUiState.Success(result)
                notifyProbeFinishedIfBackground(
                    input = normalized,
                    success = true,
                    resolvedTitle = result.title
                )
            }.onFailure { error ->
                val detail = error.message ?: "unknown"
                _probeState.value = ProbeUiState.Error("探测失败：$detail")
                notifyProbeFinishedIfBackground(
                    input = normalized,
                    success = false,
                    resolvedTitle = null,
                    detail = detail
                )
            }
        }
    }

    private fun notifyProbeFinishedIfBackground(
        input: String,
        success: Boolean,
        resolvedTitle: String?,
        detail: String? = null
    ) {
        if (isAppForeground()) {
            return
        }
        taskCompletionNotifier.notifyVideoProbeCompleted(
            input = input,
            resolvedTitle = resolvedTitle,
            success = success,
            detail = detail
        )
    }

    private fun isAppForeground(): Boolean {
        return ProcessLifecycleOwner.get()
            .lifecycle
            .currentState
            .isAtLeast(Lifecycle.State.STARTED)
    }

    fun clearProbeResult() {
        _probeState.value = ProbeUiState.Idle
        _selectedEpisodeNos.value = emptySet()
    }

    fun toggleEpisodeSelection(episodeNo: Int) {
        if (episodeNo <= 0) {
            return
        }
        _selectedEpisodeNos.value = _selectedEpisodeNos.value.toMutableSet().apply {
            if (contains(episodeNo)) {
                remove(episodeNo)
            } else {
                add(episodeNo)
            }
        }
    }

    fun selectAllEpisodes() {
        val state = _probeState.value as? ProbeUiState.Success ?: return
        _selectedEpisodeNos.value = state.result.episodes.mapNotNull {
            it.episodeNo.takeIf { episodeNo -> episodeNo > 0 }
        }.toSet()
    }

    fun invertEpisodeSelection() {
        val state = _probeState.value as? ProbeUiState.Success ?: return
        val all = state.result.episodes.mapNotNull {
            it.episodeNo.takeIf { episodeNo -> episodeNo > 0 }
        }.toSet()
        _selectedEpisodeNos.value = all - _selectedEpisodeNos.value
    }

    fun submitDetectedSingleVideo() {
        val state = _probeState.value as? ProbeUiState.Success ?: return
        val result = state.result
        val submitUrl = result.resolvedUrl.ifBlank {
            result.episodes.firstOrNull()?.episodeUrl.orEmpty()
        }
        if (submitUrl.isBlank()) {
            _events.tryEmit(CollectionUiEvent.Snackbar("缺少可提交的视频链接"))
            return
        }
        viewModelScope.launch {
            _submitInProgress.value = true
            runCatching {
                repository.submitSingleTask(videoUrl = submitUrl)
            }.onSuccess { response ->
                if (!response.success || response.taskId.isBlank()) {
                    _events.tryEmit(
                        CollectionUiEvent.Snackbar(
                            response.message.ifBlank { "提交失败，请重试" }
                        )
                    )
                    return@onSuccess
                }
                clearProbeResult()
                _events.tryEmit(
                    CollectionUiEvent.SingleTaskSubmitted(
                        taskId = response.taskId,
                        message = response.message.ifBlank { "任务已提交" }
                    )
                )
                refreshCollections()
            }.onFailure { error ->
                _events.tryEmit(
                    CollectionUiEvent.Snackbar("提交失败：${error.message ?: "unknown"}")
                )
            }.also {
                _submitInProgress.value = false
            }
        }
    }

    fun submitDetectedCollectionBatch() {
        val state = _probeState.value as? ProbeUiState.Success ?: return
        val result = state.result
        if (!result.isCollection || result.collectionId.isBlank()) {
            _events.tryEmit(CollectionUiEvent.Snackbar("当前不是可批量提交的合集"))
            return
        }
        val selected = _selectedEpisodeNos.value.sorted()
        if (selected.isEmpty()) {
            _events.tryEmit(CollectionUiEvent.Snackbar("请先选择要提交的分集"))
            return
        }
        viewModelScope.launch {
            _submitInProgress.value = true
            runCatching {
                repository.submitCollectionBatch(
                    collectionId = result.collectionId,
                    episodeNos = selected
                )
            }.onSuccess { response ->
                clearProbeResult()
                _events.tryEmit(
                    CollectionUiEvent.BatchSubmitted(
                        collectionId = response.collectionId,
                        collectionTitle = result.title.ifBlank { response.collectionId },
                        submittedCount = response.submittedCount,
                        message = response.message
                    )
                )
                refreshCollections()
            }.onFailure { error ->
                _events.tryEmit(
                    CollectionUiEvent.Snackbar("合集提交失败：${error.message ?: "unknown"}")
                )
            }.also {
                _submitInProgress.value = false
            }
        }
    }

    fun openCollectionDetail(collectionId: String) {
        val normalized = collectionId.trim()
        if (normalized.isEmpty()) {
            return
        }
        _collectionDetailId.value = normalized
        realtimeCollectionJob?.cancel()
        realtimeCollectionJob = viewModelScope.launch {
            runCatching { repository.refreshCollections() }
            repository.subscribeCollectionTasks(normalized)
        }
    }

    fun closeCollectionDetail() {
        _collectionDetailId.value = null
        realtimeCollectionJob?.cancel()
        realtimeCollectionJob = null
        repository.stopRealtime()
    }

    fun retryEpisode(collectionId: String, episode: CollectionEpisodeUi) {
        if (episode.episodeUrl.isBlank()) {
            _events.tryEmit(CollectionUiEvent.Snackbar("该分集缺少可重试链接"))
            return
        }
        viewModelScope.launch {
            runCatching {
                repository.submitSingleTask(
                    videoUrl = episode.episodeUrl,
                    collectionId = collectionId,
                    episodeNo = episode.episodeNo
                )
            }.onSuccess { response ->
                if (response.success && response.taskId.isNotBlank()) {
                    _events.tryEmit(
                        CollectionUiEvent.Snackbar("第${episode.episodeNo}集已重新提交")
                    )
                    refreshCollections()
                } else {
                    _events.tryEmit(
                        CollectionUiEvent.Snackbar(
                            response.message.ifBlank { "重试提交失败" }
                        )
                    )
                }
            }.onFailure { error ->
                _events.tryEmit(
                    CollectionUiEvent.Snackbar("重试失败：${error.message ?: "unknown"}")
                )
            }
        }
    }

    override fun onCleared() {
        super.onCleared()
        probeJob?.cancel()
        probeScope.cancel()
        repository.stopRealtime()
    }
}

class CollectionFeatureViewModelFactory(
    private val application: Application,
    private val apiBaseUrl: String
) : ViewModelProvider.Factory {
    override fun <T : ViewModel> create(modelClass: Class<T>): T {
        if (modelClass.isAssignableFrom(CollectionFeatureViewModel::class.java)) {
            @Suppress("UNCHECKED_CAST")
            return CollectionFeatureViewModel(application, apiBaseUrl) as T
        }
        throw IllegalArgumentException("Unknown ViewModel class: ${modelClass.name}")
    }
}
