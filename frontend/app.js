const state = {
  rawRows: [],
  rows: [],
  numericColumns: [],
  algorithmColumn: null,
  clusterValue: "All clusters",
  axis: { x: "", y: "", z: "" },
  pointSize: 5,
  nearClip: 0.1,
  yaw: -0.65,
  pitch: 0.35,
  distance: 3.2,
  targetDistance: 3.2,
  focusTarget: { x: 0, y: 0, z: 0 },
  targetFocusTarget: { x: 0, y: 0, z: 0 },
  hoveredIndex: -1,
  selectedIndex: -1,
  dragging: false,
  pointerMoved: false,
  lastPointer: { x: 0, y: 0 },
  projectedPoints: [],
  clusterTerritories: [],
  normalizedCoords: [],
  colors: new Map(),
  imageCache: new Map(),
  animationFrame: null,
  playlistBuilder: {
    mood: null,
    step: 0,
    picks: [],
    candidates: [],
    playlistLength: 20,
    generatedPlaylist: [],
    isGenerating: false,
    error: "",
  },
  debugLog: [],
  similarityModel: null,
  activeTab: "clustering",
};

const canvas = document.getElementById("clusterCanvas");
const ctx = canvas ? canvas.getContext("2d") : null;
const clusterMethodSelect = document.getElementById("clusterMethod");
const clusterKInput = document.getElementById("clusterK");
const clusterKValueEl = document.getElementById("clusterKValue");
const clusterKControl = document.getElementById("clusterKControl");
const clusterEpsInput = document.getElementById("clusterEps");
const clusterEpsValueEl = document.getElementById("clusterEpsValue");
const clusterEpsControl = document.getElementById("clusterEpsControl");
const clusterMinSamplesInput = document.getElementById("clusterMinSamples");
const clusterMinSamplesValueEl = document.getElementById("clusterMinSamplesValue");
const clusterMinSamplesControl = document.getElementById("clusterMinSamplesControl");
const xAxisSelect = document.getElementById("xAxis");
const yAxisSelect = document.getElementById("yAxis");
const zAxisSelect = document.getElementById("zAxis");
const clusterFilter = document.getElementById("clusterFilter");
const songCountEl = document.getElementById("songCount");
const clusterCountEl = document.getElementById("clusterCount");
const featureCountEl = document.getElementById("featureCount");
const legendEl = document.getElementById("legend");
const emptyStateEl = document.getElementById("emptyState");
const tooltipEl = document.getElementById("tooltip");
const detailCardEl = document.getElementById("detailCard");
const clusterDetailEl = document.getElementById("clusterDetail");
const resetCameraBtn = document.getElementById("resetCamera");
const clearSongFocusBtn = document.getElementById("clearSongFocus");
const playlistMenuEl = document.getElementById("playlistMenu");
const tabClusteringBtn = document.getElementById("tabClustering");
const tabPlaylistBtn = document.getElementById("tabPlaylist");
const pageMode = document.body.dataset.page || "clustering";
const DBSCAN_EPS_VALUES = ["0.7", "0.9", "1.1", "1.3", "1.5", "1.8", "2.1"];
const DBSCAN_MIN_SAMPLES_VALUES = ["4", "6", "8", "10"];

function setClearSongFocusVisibility() {
  if (clearSongFocusBtn) {
    clearSongFocusBtn.classList.toggle("hidden", state.selectedIndex === -1);
  }
}

function addDebugLog(message) {
  const timestamp = new Date().toLocaleTimeString();
  const entry = `[${timestamp}] ${message}`;
  state.debugLog = [entry, ...state.debugLog].slice(0, 40);
  console.log(entry);
}

function applyActiveTab() {
  if (pageMode === "playlist") {
    if (tabClusteringBtn) {
      tabClusteringBtn.classList.remove("active");
    }
    if (tabPlaylistBtn) {
      tabPlaylistBtn.classList.add("active");
    }
    return;
  }

  const clusteringNodes = document.querySelectorAll(".clustering-tab");
  const playlistNodes = document.querySelectorAll(".playlist-tab");
  const showClustering = state.activeTab === "clustering";

  clusteringNodes.forEach((node) => node.classList.toggle("hidden", !showClustering));
  playlistNodes.forEach((node) => node.classList.toggle("hidden", showClustering));
  if (canvas) {
    canvas.classList.toggle("hidden", !showClustering);
  }
  if (clearSongFocusBtn) {
    clearSongFocusBtn.classList.toggle("hidden", !showClustering || state.selectedIndex === -1);
  }
  if (detailCardEl) {
    detailCardEl.classList.toggle("hidden", !showClustering || state.selectedIndex === -1);
  }
  if (tooltipEl) {
    tooltipEl.classList.toggle("hidden", !showClustering || state.hoveredIndex === -1);
  }
  if (emptyStateEl) {
    emptyStateEl.classList.toggle("hidden", !showClustering || state.rows.length > 0);
  }
  if (tabClusteringBtn) {
    tabClusteringBtn.classList.toggle("active", showClustering);
  }
  if (tabPlaylistBtn) {
    tabPlaylistBtn.classList.toggle("active", !showClustering);
  }
}

function shuffle(items) {
  const copy = items.slice();
  for (let index = copy.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(Math.random() * (index + 1));
    [copy[index], copy[swapIndex]] = [copy[swapIndex], copy[index]];
  }
  return copy;
}

function getPlaylistPool() {
  const uniqueTracks = new Map();
  state.rows.forEach((row) => {
    if (!uniqueTracks.has(row.track_id)) {
      uniqueTracks.set(row.track_id, row);
    }
  });
  return [...uniqueTracks.values()];
}

function parseModeValue(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "major") {
    return 1;
  }
  if (normalized === "minor") {
    return 0;
  }
  return 0;
}

function parseCamelotValue(value) {
  const match = String(value || "").trim().match(/^(\d{1,2})([AB])$/i);
  if (!match) {
    return { number: 0, letter: 0 };
  }
  return {
    number: Math.max(0, Math.min(11, Number(match[1]) - 1)),
    letter: match[2].toUpperCase() === "B" ? 1 : 0,
  };
}

function keyToPitchClass(value) {
  const lookup = {
    C: 0, "C#": 1, Db: 1, D: 2, "D#": 3, Eb: 3, E: 4, F: 5, "F#": 6, Gb: 6,
    G: 7, "G#": 8, Ab: 8, A: 9, "A#": 10, Bb: 10, B: 11,
  };
  return lookup[String(value || "").trim()] ?? 0;
}

function buildSimilarityModel() {
  addDebugLog(`buildSimilarityModel:start rows=${state.rows.length}`);
  const tracks = getPlaylistPool();
  if (!tracks.length) {
    state.similarityModel = null;
    addDebugLog("buildSimilarityModel:empty");
    return;
  }

  const vectors = tracks.map((track) => {
    const camelot = parseCamelotValue(track.camelot);
    const pitchClass = keyToPitchClass(track.key);
    return [
      Number(track.bpm) || 0,
      Number(track.popularity) || 0,
      Number(track.loudness) || 0,
      Number(track.acousticness) || 0,
      Number(track.danceability) || 0,
      Number(track.energy) || 0,
      Number(track.instrumentalness) || 0,
      Number(track.liveness) || 0,
      Number(track.speechiness) || 0,
      Number(track.valence) || 0,
      Math.sin((2 * Math.PI * pitchClass) / 12),
      Math.cos((2 * Math.PI * pitchClass) / 12),
      parseModeValue(track.mode),
      Math.sin((2 * Math.PI * camelot.number) / 12),
      Math.cos((2 * Math.PI * camelot.number) / 12),
      camelot.letter,
    ];
  });

  const dimensions = vectors[0].length;
  const means = Array.from({ length: dimensions }, (_, index) => (
    vectors.reduce((sum, vector) => sum + vector[index], 0) / vectors.length
  ));
  const stds = Array.from({ length: dimensions }, (_, index) => {
    const variance = vectors.reduce((sum, vector) => sum + ((vector[index] - means[index]) ** 2), 0) / vectors.length;
    return Math.sqrt(variance) || 1;
  });

  const normalizedVectors = vectors.map((vector) => {
    const standardized = vector.map((value, index) => (value - means[index]) / stds[index]);
    const magnitude = Math.sqrt(standardized.reduce((sum, value) => sum + value * value, 0)) || 1;
    return standardized.map((value) => value / magnitude);
  });

  state.similarityModel = { tracks, normalizedVectors };
  addDebugLog(`buildSimilarityModel:done tracks=${tracks.length}`);
}

function numericValue(track, key) {
  return Number(track[key]) || 0;
}

function moodTargetVector(mood) {
  const targets = {
    study: {
      bpm: 90, popularity: 45, loudness: 40, acousticness: 65, danceability: 45,
      energy: 25, instrumentalness: 80, liveness: 20, speechiness: 10, valence: 50,
    },
    workout: {
      bpm: 145, popularity: 72, loudness: 82, acousticness: 18, danceability: 82,
      energy: 88, instrumentalness: 20, liveness: 28, speechiness: 18, valence: 72,
    },
    sleep: {
      bpm: 65, popularity: 38, loudness: 22, acousticness: 58, danceability: 28,
      energy: 14, instrumentalness: 84, liveness: 14, speechiness: 8, valence: 32,
    },
    party: {
      bpm: 128, popularity: 82, loudness: 86, acousticness: 18, danceability: 84,
      energy: 88, instrumentalness: 14, liveness: 32, speechiness: 16, valence: 82,
    },
  };
  return targets[mood] || targets.study;
}

async function buildGeneratedPlaylist(mood, picks, desiredLength) {
  addDebugLog(`buildGeneratedPlaylist:start mood=${mood} seeds=${picks.length} desired=${desiredLength}`);
  if (!picks.length) {
    addDebugLog("buildGeneratedPlaylist:aborted missing-seeds");
    return [];
  }

  const response = await fetch("/api/generate_playlist", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      mood,
      seed_tracks: picks,
      playlist_length: desiredLength,
    }),
  });

  if (!response.ok) {
    const errorPayload = await response.json().catch(() => ({ error: "Playlist API request failed." }));
    throw new Error(errorPayload.error || "Playlist API request failed.");
  }

  const payload = await response.json();
  addDebugLog(`buildGeneratedPlaylist:done produced=${payload.playlist?.length || 0}`);
  return payload.playlist || [];
}

function moodScore(track, mood) {
  const energy = Number(track.energy) || 0;
  const instrumentalness = Number(track.instrumentalness) || 0;
  const speechiness = Number(track.speechiness) || 0;
  const acousticness = Number(track.acousticness) || 0;
  const valence = Number(track.valence) || 0;
  const danceability = Number(track.danceability) || 0;
  const popularity = Number(track.popularity) || 0;
  const bpm = Number(track.bpm) || 0;
  const loudness = Number(track.loudness) || 0;

  const rangeScore = (value, low, high) => {
    if (value >= low && value <= high) {
      return 1;
    }
    const midpoint = (low + high) / 2;
    const span = Math.max((high - low) / 2, 1);
    return Math.max(0, 1 - Math.abs(value - midpoint) / span);
  };

  if (mood === "study") {
    return (
      rangeScore(energy, 0, 45) +
      rangeScore(instrumentalness, 60, 100) +
      rangeScore(speechiness, 0, 20) +
      rangeScore(acousticness, 40, 100) +
      rangeScore(valence, 40, 60)
    ) / 5;
  }

  if (mood === "workout") {
    return (
      rangeScore(energy, 75, 100) +
      rangeScore(danceability, 70, 100) +
      rangeScore(bpm, 120, 180) +
      rangeScore(loudness, 70, 100) +
      rangeScore(valence, 50, 100)
    ) / 5;
  }

  if (mood === "sleep") {
    return (
      rangeScore(energy, 0, 30) +
      rangeScore(bpm, 0, 80) +
      rangeScore(loudness, 0, 60) +
      rangeScore(instrumentalness, 70, 100) +
      rangeScore(valence, 0, 50)
    ) / 5;
  }

  if (mood === "party") {
    return (
      rangeScore(energy, 75, 100) +
      rangeScore(danceability, 70, 100) +
      rangeScore(valence, 65, 100) +
      rangeScore(popularity, 70, 100) +
      rangeScore(loudness, 70, 100)
    ) / 5;
  }

  return 0;
}

function cosineSimilarity(left, right) {
  let total = 0;
  for (let index = 0; index < left.length; index += 1) {
    total += left[index] * right[index];
  }
  return total;
}

function getPlaylistCandidates(mood, previousPick = null) {
  addDebugLog(`getPlaylistCandidates:start mood=${mood} previous=${previousPick ? previousPick.track_id : "none"}`);
  if (!state.similarityModel) {
    addDebugLog("getPlaylistCandidates:aborted missing-model");
    return [];
  }

  const taken = new Set(state.playlistBuilder.picks.map((pick) => pick.track_id));
  const { tracks, normalizedVectors } = state.similarityModel;

  if (!previousPick) {
    const ranked = tracks
      .map((track, index) => ({ track, score: moodScore(track, mood), index }))
      .sort((left, right) => right.score - left.score)
      .filter(({ track }) => !taken.has(track.track_id));

    const strongMatches = ranked.filter(({ score }) => score >= 0.72).slice(0, 9);
    const fallbackMatches = ranked.slice(0, 9);
    const result = shuffle(strongMatches.length >= 3 ? strongMatches : fallbackMatches)
      .slice(0, 3)
      .map(({ track }) => track);
    addDebugLog(`getPlaylistCandidates:done initial count=${result.length}`);
    return result;
  }

  const seedIndex = tracks.findIndex((track) => track.track_id === previousPick.track_id);
  if (seedIndex === -1) {
    addDebugLog("getPlaylistCandidates:aborted seed-not-found");
    return [];
  }

  const seedVector = normalizedVectors[seedIndex];
  const result = tracks
    .map((track, index) => ({
      track,
      score: 0.72 * cosineSimilarity(seedVector, normalizedVectors[index]) + 0.28 * moodScore(track, mood),
    }))
    .filter(({ track }) => !taken.has(track.track_id) && track.track_id !== previousPick.track_id)
    .sort((left, right) => right.score - left.score)
    .slice(0, 9)
    .sort(() => Math.random() - 0.5)
    .slice(0, 3)
    .map(({ track }) => track);
  addDebugLog(`getPlaylistCandidates:done followup count=${result.length}`);
  return result;
}

function renderPlaylistBuilder() {
  if (!playlistMenuEl) {
    return;
  }
  const { mood, step, picks, candidates, error } = state.playlistBuilder;
  const debugMarkup = state.debugLog.length
    ? `<div class="playlist-debug-log">${state.debugLog.map((line) => `<div>${escapeHtml(line)}</div>`).join("")}</div>`
    : "";

  if (!mood) {
    playlistMenuEl.innerHTML = `
      <section class="playlist-stage playlist-stage-centered">
        <div class="playlist-stage-copy">
          <h2>I want to...</h2>
          <p>${state.rows.length
            ? ""
            : "This page loads tracks from clustering/all_tracks.csv. Import music first if the song list is empty."
          }</p>
          ${error ? `<p class="playlist-inline-error">${escapeHtml(error)}</p>` : ""}
        </div>
        <div class="playlist-chip-row playlist-chip-row-large">
          <button class="playlist-chip playlist-chip-hero" data-mood="study" ${state.rows.length ? "" : "disabled"}>Study</button>
          <button class="playlist-chip playlist-chip-hero" data-mood="workout" ${state.rows.length ? "" : "disabled"}>Workout</button>
          <button class="playlist-chip playlist-chip-hero" data-mood="sleep" ${state.rows.length ? "" : "disabled"}>Sleep</button>
          <button class="playlist-chip playlist-chip-hero" data-mood="party" ${state.rows.length ? "" : "disabled"}>Party</button>
        </div>
        ${debugMarkup}
      </section>
    `;
    return;
  }

  if (step >= 5) {
    const generatedContinuation = state.playlistBuilder.generatedPlaylist.slice(picks.length);
    playlistMenuEl.innerHTML = `
      <section class="playlist-stage">
        <div class="playlist-stage-copy playlist-stage-copy-wide">
          <h2>I want to... ${escapeHtml(mood)}</h2>
          <p>Your 5-song seed is ready. Pick a final playlist length and generate a fuller playlist from the whole loaded song set.</p>
          ${error ? `<p class="playlist-inline-error">${escapeHtml(error)}</p>` : ""}
        </div>
        <div class="playlist-selected-list playlist-selected-list-grid">
          ${picks.map((pick, index) => `
            <div class="playlist-selected-item playlist-selected-item-with-art">
              ${pick.album_image_url
                ? `<img class="playlist-selected-cover" src="${escapeHtml(pick.album_image_url)}" alt="">`
                : `<div class="playlist-selected-cover playlist-fallback">Seed</div>`}
              <div>
                <strong>${index + 1}. ${escapeHtml(pick.track_name || "Unknown track")}</strong><br>
                ${escapeHtml(pick.artist_names || "Unknown artist")}
              </div>
            </div>
          `).join("")}
        </div>
        <div class="playlist-length-row playlist-length-row-large">
          <label>
            <span>Playlist Length (max 100)</span>
            <input id="playlistLengthInput" type="number" min="5" max="100" value="${state.playlistBuilder.playlistLength}">
          </label>
        </div>
        <div class="playlist-chip-row playlist-chip-row-large">
          <button class="playlist-chip playlist-chip-hero${state.playlistBuilder.isGenerating ? " active" : ""}" data-action="generate-playlist" ${state.playlistBuilder.isGenerating ? "disabled" : ""}>
            ${state.playlistBuilder.isGenerating ? "Generating..." : "Generate playlist"}
          </button>
          <button class="playlist-chip playlist-chip-hero playlist-chip-secondary" data-action="restart-playlist">Start over</button>
        </div>
        ${generatedContinuation.length ? `
          <div class="playlist-selected-list playlist-selected-list-grid">
            ${generatedContinuation.map((pick, index) => `
              <div class="playlist-selected-item playlist-selected-item-with-art">
                ${pick.album_image_url
                  ? `<img class="playlist-selected-cover" src="${escapeHtml(pick.album_image_url)}" alt="">`
                  : `<div class="playlist-selected-cover playlist-fallback">Mix</div>`}
                <div>
                  <strong>${picks.length + index + 1}. ${escapeHtml(pick.track_name || "Unknown track")}</strong><br>
                  ${escapeHtml(pick.artist_names || "Unknown artist")}
                </div>
              </div>
            `).join("")}
          </div>
        ` : ""}
        ${debugMarkup}
      </section>
    `;
    return;
  }

  playlistMenuEl.innerHTML = `
    <section class="playlist-stage playlist-stage-centered">
      <div class="playlist-stage-copy playlist-stage-copy-wide">
        <h2>I want to... ${escapeHtml(mood)}</h2>
        <p>Pick 1 of 3. Round ${step + 1} of 5.</p>
        ${error ? `<p class="playlist-inline-error">${escapeHtml(error)}</p>` : ""}
      </div>
      <div class="playlist-choice-grid playlist-choice-grid-large">
        ${candidates.map((song) => `
          <button class="playlist-choice playlist-choice-large" data-track-id="${escapeHtml(song.track_id)}">
            ${song.album_image_url
              ? `<img src="${escapeHtml(song.album_image_url)}" alt="">`
              : `<div class="playlist-fallback">Pick</div>`}
            <span>
              <strong>${escapeHtml(song.track_name || "Unknown track")}</strong>
              ${escapeHtml(song.artist_names || "Unknown artist")}
            </span>
          </button>
        `).join("")}
      </div>
      <div class="playlist-selected-list playlist-selected-list-grid">
        ${picks.map((pick, index) => `
          <div class="playlist-selected-item playlist-selected-item-with-art">
            ${pick.album_image_url
              ? `<img class="playlist-selected-cover" src="${escapeHtml(pick.album_image_url)}" alt="">`
              : `<div class="playlist-selected-cover playlist-fallback">Seed</div>`}
            <div>
              <strong>${index + 1}. ${escapeHtml(pick.track_name || "Unknown track")}</strong><br>
              ${escapeHtml(pick.artist_names || "Unknown artist")}
            </div>
          </div>
        `).join("")}
      </div>
      <div class="playlist-chip-row playlist-chip-row-large">
        <button class="playlist-chip playlist-chip-hero playlist-chip-secondary" data-action="restart-playlist">Start over</button>
      </div>
      ${debugMarkup}
    </section>
  `;
}

function startPlaylistBuilder(mood) {
  addDebugLog(`startPlaylistBuilder:mood=${mood}`);
  if (!state.rows.length) {
    state.playlistBuilder = {
      mood: null,
      step: 0,
      picks: [],
      candidates: [],
      playlistLength: 20,
      generatedPlaylist: [],
      isGenerating: false,
      error: "Import music first so clustering/all_tracks.csv has songs to pick from.",
    };
    renderPlaylistBuilder();
    return;
  }

  state.playlistBuilder = {
    mood,
    step: 0,
    picks: [],
    candidates: getPlaylistCandidates(mood),
    playlistLength: 20,
    generatedPlaylist: [],
    isGenerating: false,
    error: "",
  };
  renderPlaylistBuilder();
}

function choosePlaylistSong(trackId) {
  addDebugLog(`choosePlaylistSong:track_id=${trackId}`);
  const choice = state.playlistBuilder.candidates.find((song) => song.track_id === trackId);
  if (!choice) {
    addDebugLog("choosePlaylistSong:choice-not-found");
    return;
  }

  const picks = [...state.playlistBuilder.picks, choice];
  const nextStep = picks.length;
  state.playlistBuilder = {
    ...state.playlistBuilder,
    picks,
    step: nextStep,
    candidates: nextStep >= 5 ? [] : getPlaylistCandidates(state.playlistBuilder.mood, choice),
    generatedPlaylist: [],
    isGenerating: false,
  };
  renderPlaylistBuilder();
}

function lerp(start, end, amount) {
  return start + (end - start) * amount;
}

function cameraNeedsAnimation() {
  const focusDelta =
    Math.abs(state.focusTarget.x - state.targetFocusTarget.x) +
    Math.abs(state.focusTarget.y - state.targetFocusTarget.y) +
    Math.abs(state.focusTarget.z - state.targetFocusTarget.z);
  const distanceDelta = Math.abs(state.distance - state.targetDistance);
  return focusDelta > 0.001 || distanceDelta > 0.001;
}

function stepCamera() {
  state.focusTarget = {
    x: lerp(state.focusTarget.x, state.targetFocusTarget.x, 0.14),
    y: lerp(state.focusTarget.y, state.targetFocusTarget.y, 0.14),
    z: lerp(state.focusTarget.z, state.targetFocusTarget.z, 0.14),
  };
  state.distance = lerp(state.distance, state.targetDistance, 0.14);
}

function requestDraw() {
  if (state.animationFrame !== null) {
    return;
  }

  const frame = () => {
    stepCamera();
    if (state.activeTab === "clustering") {
      drawScene();
    }
    setClearSongFocusVisibility();

    if (cameraNeedsAnimation()) {
      state.animationFrame = window.requestAnimationFrame(frame);
    } else {
      state.focusTarget = { ...state.targetFocusTarget };
      state.distance = state.targetDistance;
      if (state.activeTab === "clustering") {
        drawScene();
      }
      setClearSongFocusVisibility();
      state.animationFrame = null;
    }
  };

  state.animationFrame = window.requestAnimationFrame(frame);
}

function resizeCanvas() {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = Math.round(rect.width * dpr);
  canvas.height = Math.round(rect.height * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  requestDraw();
}

function parseCsv(text) {
  const rows = [];
  let current = "";
  let row = [];
  let insideQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"') {
      if (insideQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        insideQuotes = !insideQuotes;
      }
      continue;
    }

    if (char === "," && !insideQuotes) {
      row.push(current);
      current = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !insideQuotes) {
      if (char === "\r" && next === "\n") {
        i += 1;
      }
      row.push(current);
      current = "";
      if (row.some((value) => value !== "")) {
        rows.push(row);
      }
      row = [];
      continue;
    }

    current += char;
  }

  row.push(current);
  if (row.some((value) => value !== "")) {
    rows.push(row);
  }

  const [header, ...body] = rows;
  return body.map((values) => {
    const entry = {};
    header.forEach((column, index) => {
      entry[column] = values[index] ?? "";
    });
    return entry;
  });
}

function inferNumericColumns(rows) {
  if (!rows.length) {
    return [];
  }

  const columns = Object.keys(rows[0]);
  return columns.filter((column) => {
    const nonEmptyValues = rows
      .map((row) => row[column])
      .filter((value) => value !== "" && value !== null && value !== undefined);

    if (!nonEmptyValues.length) {
      return false;
    }

    return nonEmptyValues.every((value) => Number.isFinite(Number(value)));
  });
}

function detectAlgorithmColumn(rows) {
  if (!rows.length) {
    return null;
  }

  if (Object.prototype.hasOwnProperty.call(rows[0], "algorithm")) {
    return "algorithm";
  }

  return null;
}

function setSelectOptions(select, options, preferredValue) {
  select.innerHTML = "";
  options.forEach((optionValue) => {
    const option = document.createElement("option");
    option.value = optionValue;
    option.textContent = optionValue;
    if (optionValue === preferredValue) {
      option.selected = true;
    }
    select.appendChild(option);
  });
}

function initializeControls() {
  if (!xAxisSelect || !yAxisSelect || !zAxisSelect) {
    return;
  }
  const preferredAxes = ["danceability", "energy", "valence"];
  const fallbackAxes = state.numericColumns.slice(0, 3);
  const selectedAxes = preferredAxes.map((axis, index) => (
    state.numericColumns.includes(axis) ? axis : fallbackAxes[index]
  ));

  state.axis = {
    x: selectedAxes[0] || "",
    y: selectedAxes[1] || "",
    z: selectedAxes[2] || "",
  };

  setSelectOptions(xAxisSelect, state.numericColumns, state.axis.x);
  setSelectOptions(yAxisSelect, state.numericColumns, state.axis.y);
  setSelectOptions(zAxisSelect, state.numericColumns, state.axis.z);

  const algorithmValues = state.algorithmColumn
    ? ["All algorithms", ...new Set(state.rawRows.map((row) => row[state.algorithmColumn]).filter(Boolean))]
    : ["Single file"];

  refreshClusterFilter();
}

function filterRows() {
  state.rows = state.rawRows
    .map((row) => {
      const nextRow = { ...row };
      state.numericColumns.forEach((column) => {
        nextRow[column] = Number(row[column]);
      });
      return nextRow;
    });
}

function getVisibleBaseRows() {
  return state.rawRows;
}

function refreshClusterFilter() {
  if (!clusterFilter) {
    return;
  }
  const clusterValues = ["All clusters", ...new Set(getVisibleBaseRows().map((row) => String(row.cluster ?? "unclustered")))];
  const nextValue = clusterValues.includes(state.clusterValue) ? state.clusterValue : "All clusters";
  state.clusterValue = nextValue;
  setSelectOptions(clusterFilter, clusterValues, state.clusterValue);
}

async function loadCsvFromPath(path, options = {}) {
  addDebugLog(`loadCsvFromPath:start path=${path}`);
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Unable to load ${path}`);
  }
  const text = await response.text();
  loadRowsFromCsv(parseCsv(text), options);
  addDebugLog(`loadCsvFromPath:done path=${path} rows=${state.rawRows.length}`);
}

function loadRowsFromCsv(parsedRows, options = {}) {
  const { enableClustering = false } = options;
  state.rawRows = parsedRows;
  state.numericColumns = inferNumericColumns(state.rawRows)
    .filter((column) => !["track_id"].includes(column));
  state.algorithmColumn = detectAlgorithmColumn(state.rawRows);
  filterRows();
  state.selectedIndex = -1;
  state.hoveredIndex = -1;
  state.targetFocusTarget = { x: 0, y: 0, z: 0 };
  state.targetDistance = 3.2;

  if (enableClustering) {
    initializeControls();
    renderClusterDetail();
    requestDraw();
  }

  state.playlistBuilder = {
    mood: null,
    step: 0,
    picks: [],
    candidates: [],
    playlistLength: 20,
    generatedPlaylist: [],
    isGenerating: false,
    error: "",
  };
  updateStats();
}

function syncClusteringFileControls() {
  if (!clusterMethodSelect) {
    return;
  }

  const method = clusterMethodSelect.value;
  const usesK = method !== "dbscan";
  if (clusterKControl) {
    clusterKControl.classList.toggle("hidden", !usesK);
  }
  if (clusterEpsControl) {
    clusterEpsControl.classList.toggle("hidden", usesK);
  }
  if (clusterMinSamplesControl) {
    clusterMinSamplesControl.classList.toggle("hidden", usesK);
  }

  if (clusterKValueEl && clusterKInput) {
    clusterKValueEl.textContent = clusterKInput.value;
  }
  if (clusterEpsValueEl && clusterEpsInput) {
    clusterEpsValueEl.textContent = DBSCAN_EPS_VALUES[Number(clusterEpsInput.value)] || DBSCAN_EPS_VALUES[0];
  }
  if (clusterMinSamplesValueEl && clusterMinSamplesInput) {
    clusterMinSamplesValueEl.textContent = DBSCAN_MIN_SAMPLES_VALUES[Number(clusterMinSamplesInput.value)] || DBSCAN_MIN_SAMPLES_VALUES[0];
  }
}

function getSelectedClusteringFile() {
  const method = clusterMethodSelect?.value || "agglomerative";
  if (method === "dbscan") {
    const eps = DBSCAN_EPS_VALUES[Number(clusterEpsInput?.value || 0)] || DBSCAN_EPS_VALUES[0];
    const minSamples = DBSCAN_MIN_SAMPLES_VALUES[Number(clusterMinSamplesInput?.value || 0)] || DBSCAN_MIN_SAMPLES_VALUES[0];
    return `dbscan_eps${eps}_min${minSamples}_assignments.csv`;
  }

  const k = clusterKInput?.value || "2";
  return `${method}_k${k}_assignments.csv`;
}

async function loadSelectedClusteringFile() {
  syncClusteringFileControls();
  const filename = getSelectedClusteringFile();
  await loadCsvFromPath(`/clustering/${filename}`, { enableClustering: true });
}

async function loadDefaultPlaylistDataset() {
  await loadCsvFromPath("/clustering/all_tracks.csv", { enableClustering: false });
}

function normalizeValues(column) {
  const values = state.rows.map((row) => row[column]).filter(Number.isFinite);
  const boundedHundredScale = values.length > 0 && values.every((value) => value >= 0 && value <= 100);

  if (boundedHundredScale) {
    return state.rows.map((row) => (row[column] / 50) - 1);
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === max) {
    return state.rows.map(() => 0);
  }
  return state.rows.map((row) => ((row[column] - min) / (max - min)) * 2 - 1);
}

function hslColor(index, total) {
  const hue = (index * 360) / Math.max(total, 1);
  return `hsl(${hue}, 78%, 62%)`;
}

function desaturateColor(color, amount = 0.8) {
  if (color.startsWith("hsl")) {
    const match = color.match(/hsl\(([^,]+),\s*([^,]+)%,\s*([^)]+)%\)/);
    if (!match) {
      return color;
    }
    const [, hue, saturation, lightness] = match;
    const nextSaturation = Math.max(0, Number(saturation) * (1 - amount));
    const nextLightness = Math.min(82, Number(lightness) + amount * 10);
    return `hsl(${hue}, ${nextSaturation}%, ${nextLightness}%)`;
  }

  return color;
}

function refreshLegend() {
  legendEl.innerHTML = "";
  const clusters = [...new Set(getVisibleBaseRows().map((row) => String(row.cluster ?? "unclustered")))];
  const sortedClusters = clusters.sort((a, b) => {
    const aNum = Number(a);
    const bNum = Number(b);
    if (Number.isNaN(aNum) || Number.isNaN(bNum)) {
      return a.localeCompare(b);
    }
    return aNum - bNum;
  });

  state.colors = new Map();
  sortedClusters.forEach((cluster, index) => {
    const color = cluster === "-1" ? "#6a7b86" : hslColor(index, sortedClusters.length);
    state.colors.set(cluster, color);

    const count = getVisibleBaseRows().filter((row) => String(row.cluster) === cluster).length;
    const item = document.createElement("div");
    item.className = `legend-item${state.clusterValue === cluster ? " active" : ""}`;
    item.innerHTML = `<span class="swatch" style="background:${color}"></span><span>Cluster ${cluster} (${count})</span>`;
    item.addEventListener("click", () => {
      state.clusterValue = state.clusterValue === cluster ? "All clusters" : cluster;
      clusterFilter.value = state.clusterValue;
      updateFromControls();
    });
    legendEl.appendChild(item);
  });

  clusterCountEl.textContent = String(sortedClusters.length);
}

function updateStats() {
  addDebugLog("updateStats");
  if (songCountEl) {
    songCountEl.textContent = String(state.rows.length);
  }
  if (featureCountEl) {
    featureCountEl.textContent = String(state.numericColumns.length);
  }
  buildSimilarityModel();
  if (legendEl) {
    refreshLegend();
  }
  renderPlaylistBuilder();
  applyActiveTab();
}

function hexToRgba(color, alpha) {
  if (color.startsWith("hsl")) {
    return color.replace("hsl", "hsla").replace(")", `, ${alpha})`);
  }

  const normalized = color.replace("#", "");
  const step = normalized.length === 3 ? 1 : 2;
  const parts = [];
  for (let i = 0; i < normalized.length; i += step) {
    const token = normalized.slice(i, i + step);
    const expanded = step === 1 ? token + token : token;
    parts.push(parseInt(expanded, 16));
  }
  return `rgba(${parts[0]}, ${parts[1]}, ${parts[2]}, ${alpha})`;
}

function getAlbumImage(url) {
  if (!url) {
    return null;
  }

  if (state.imageCache.has(url)) {
    return state.imageCache.get(url);
  }

  const image = new Image();
  image.crossOrigin = "anonymous";
  image.decoding = "async";
  image.loading = "eager";
  image.src = url;
  image.addEventListener("load", () => requestDraw());
  image.addEventListener("error", () => requestDraw());
  state.imageCache.set(url, image);
  return image;
}

function drawAlbumCoverPoint(point, fill, radius) {
  const image = getAlbumImage(point.row.album_image_url);
  const ringWidth = Math.max(1.2, radius * 0.24);
  const innerRadius = Math.max(1, radius - ringWidth);

  ctx.save();
  ctx.beginPath();
  ctx.fillStyle = fill;
  ctx.arc(point.screenX, point.screenY, radius, 0, Math.PI * 2);
  ctx.fill();

  if (image && image.complete && image.naturalWidth > 0) {
    ctx.beginPath();
    ctx.arc(point.screenX, point.screenY, innerRadius, 0, Math.PI * 2);
    ctx.clip();
    ctx.drawImage(
      image,
      point.screenX - innerRadius,
      point.screenY - innerRadius,
      innerRadius * 2,
      innerRadius * 2
    );
  } else {
    ctx.beginPath();
    ctx.fillStyle = "rgba(255,255,255,0.14)";
    ctx.arc(point.screenX, point.screenY, innerRadius, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.restore();
}

function rotationMatrix(point) {
  const cosy = Math.cos(state.yaw);
  const siny = Math.sin(state.yaw);
  const cosp = Math.cos(state.pitch);
  const sinp = Math.sin(state.pitch);

  const x1 = point.x * cosy - point.z * siny;
  const z1 = point.x * siny + point.z * cosy;
  const y2 = point.y * cosp - z1 * sinp;
  const z2 = point.y * sinp + z1 * cosp;

  return { x: x1, y: y2, z: z2 };
}

function updateNormalizedCoords() {
  if (!state.rows.length || !state.axis.x || !state.axis.y || !state.axis.z) {
    state.normalizedCoords = [];
    return;
  }

  const normX = normalizeValues(state.axis.x);
  const normY = normalizeValues(state.axis.y);
  const normZ = normalizeValues(state.axis.z);
  state.normalizedCoords = state.rows.map((row, index) => ({
    row,
    index,
    x: normX[index],
    y: normY[index],
    z: normZ[index],
  }));
}

function projectPoints() {
  if (!state.rows.length || !state.axis.x || !state.axis.y || !state.axis.z) {
    state.projectedPoints = [];
    state.normalizedCoords = [];
    return;
  }

  updateNormalizedCoords();

  const width = canvas.clientWidth;
  const height = canvas.clientHeight;

  state.projectedPoints = state.normalizedCoords.map((coord) => {
    const rotated = rotationMatrix({
      x: coord.x - state.focusTarget.x,
      y: coord.y - state.focusTarget.y,
      z: coord.z - state.focusTarget.z,
    });
    const depth = rotated.z + state.distance;
    if (depth <= state.nearClip) {
      return null;
    }
    const perspective = 420 / depth;
    return {
      row: coord.row,
      index: coord.index,
      depth,
      worldX: coord.x,
      worldY: coord.y,
      worldZ: coord.z,
      screenX: width * 0.5 + rotated.x * perspective,
      screenY: height * 0.5 - rotated.y * perspective,
      size: Math.max(0.8, state.pointSize * (perspective / 220)),
    };
  }).filter(Boolean).sort((a, b) => b.depth - a.depth);
}

function cross(o, a, b) {
  return (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
}

function buildConvexHull(points) {
  if (points.length <= 1) {
    return points.slice();
  }

  const sorted = [...points].sort((a, b) => (a.x === b.x ? a.y - b.y : a.x - b.x));
  const lower = [];
  sorted.forEach((point) => {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], point) <= 0) {
      lower.pop();
    }
    lower.push(point);
  });

  const upper = [];
  for (let i = sorted.length - 1; i >= 0; i -= 1) {
    const point = sorted[i];
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], point) <= 0) {
      upper.pop();
    }
    upper.push(point);
  }

  lower.pop();
  upper.pop();
  return lower.concat(upper);
}

function expandHull(points, amount) {
  const center = points.reduce(
    (acc, point) => ({ x: acc.x + point.x, y: acc.y + point.y }),
    { x: 0, y: 0 }
  );
  center.x /= points.length;
  center.y /= points.length;

  return points.map((point) => {
    const dx = point.x - center.x;
    const dy = point.y - center.y;
    const length = Math.sqrt(dx * dx + dy * dy) || 1;
    return {
      x: point.x + (dx / length) * amount,
      y: point.y + (dy / length) * amount,
    };
  });
}

function smoothPolygon(points, iterations = 2) {
  if (points.length < 3) {
    return points.slice();
  }

  let result = points.slice();
  for (let step = 0; step < iterations; step += 1) {
    const refined = [];
    for (let index = 0; index < result.length; index += 1) {
      const current = result[index];
      const next = result[(index + 1) % result.length];
      refined.push({
        x: current.x * 0.75 + next.x * 0.25,
        y: current.y * 0.75 + next.y * 0.25,
      });
      refined.push({
        x: current.x * 0.25 + next.x * 0.75,
        y: current.y * 0.25 + next.y * 0.75,
      });
    }
    result = refined;
  }

  return result;
}

function buildSmoothPath(points) {
  if (!points.length) {
    return null;
  }

  const path = new Path2D();
  if (points.length < 3) {
    const pivot = points[0];
    path.arc(pivot.x, pivot.y, 24, 0, Math.PI * 2);
    return path;
  }

  const smoothedPoints = smoothPolygon(points, 3);
  const padded = smoothedPoints.map((point, index) => {
    const next = smoothedPoints[(index + 1) % smoothedPoints.length];
    return {
      current: point,
      mid: {
        x: (point.x + next.x) / 2,
        y: (point.y + next.y) / 2,
      },
    };
  });

  path.moveTo(padded[0].mid.x, padded[0].mid.y);
  padded.forEach(({ current }, index) => {
    const nextMid = padded[(index + 1) % padded.length].mid;
    path.quadraticCurveTo(current.x, current.y, nextMid.x, nextMid.y);
  });
  path.closePath();
  return path;
}

function buildClusterTerritories() {
  if (!state.rows.length || !state.axis.x || !state.axis.y || !state.axis.z) {
    state.clusterTerritories = [];
    return;
  }

  const grouped = new Map();
  state.projectedPoints.forEach((point) => {
    const cluster = String(point.row.cluster ?? "unclustered");
    if (!grouped.has(cluster)) {
      grouped.set(cluster, []);
    }
    grouped.get(cluster).push(point);
  });

  state.clusterTerritories = [...grouped.entries()].map(([cluster, points]) => {
    const centerDepth = points.reduce((sum, point) => sum + point.depth, 0) / points.length;
    const hullPoints = buildConvexHull(points.map((point) => ({ x: point.screenX, y: point.screenY })));
    const padding = Math.max(14, Math.min(38, 10 + Math.sqrt(points.length) * 1.8));
    const expandedHull = expandHull(hullPoints, padding);
    return {
      cluster,
      count: points.length,
      depth: centerDepth,
      path: points.length >= 2 ? buildSmoothPath(expandedHull) : null,
      expandedHull,
    };
  }).sort((a, b) => b.depth - a.depth);
}

function drawAxes() {
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const center = { x: width * 0.5, y: height * 0.5 };
  const axisLength = Math.min(width, height) * 0.12;
  const axes = [
    {
      label: state.axis.x,
      positiveLabel: `+${state.axis.x}`,
      negativeLabel: `-${state.axis.x}`,
      vector: { x: 1, y: 0, z: 0 },
      color: "#ff8d6b",
    },
    {
      label: state.axis.y,
      positiveLabel: `+${state.axis.y}`,
      negativeLabel: `-${state.axis.y}`,
      vector: { x: 0, y: 1, z: 0 },
      color: "#79e0c2",
    },
    {
      label: state.axis.z,
      positiveLabel: `+${state.axis.z}`,
      negativeLabel: `-${state.axis.z}`,
      vector: { x: 0, y: 0, z: 1 },
      color: "#72b4ff",
    },
  ];

  ctx.lineWidth = 1.4;
  ctx.font = "12px Segoe UI";

  axes.forEach((axis) => {
    const rotated = rotationMatrix(axis.vector);
    ctx.strokeStyle = axis.color;
    ctx.fillStyle = axis.color;
    ctx.globalAlpha = 0.22;
    ctx.beginPath();
    ctx.moveTo(center.x - rotated.x * axisLength, center.y + rotated.y * axisLength);
    ctx.lineTo(center.x + rotated.x * axisLength, center.y - rotated.y * axisLength);
    ctx.stroke();
    ctx.fillText(
      axis.positiveLabel,
      center.x + rotated.x * axisLength * 1.08,
      center.y - rotated.y * axisLength * 1.08
    );
    ctx.fillText(
      axis.negativeLabel,
      center.x - rotated.x * axisLength * 1.18,
      center.y + rotated.y * axisLength * 1.18
    );
  });
  ctx.globalAlpha = 1;
}

function drawClusterTerritories() {
  state.clusterTerritories.forEach((territory) => {
    if (!territory.path) {
      return;
    }

    const isFocused = state.clusterValue === "All clusters" || state.clusterValue === territory.cluster;
    const baseColor = state.colors.get(territory.cluster) || "#ffffff";
    const color = isFocused ? baseColor : desaturateColor(baseColor, 0.9);
    const xs = territory.expandedHull.map((point) => point.x);
    const ys = territory.expandedHull.map((point) => point.y);
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs);
    const minY = Math.min(...ys);
    const maxY = Math.max(...ys);
    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;
    const radius = Math.max(maxX - minX, maxY - minY) * 0.65;

    const glow = ctx.createRadialGradient(
      centerX - radius * 0.22,
      centerY - radius * 0.18,
      radius * 0.08,
      centerX,
      centerY,
      radius
    );
    glow.addColorStop(0, hexToRgba(color, 0.2));
    glow.addColorStop(0.5, hexToRgba(color, 0.11));
    glow.addColorStop(1, hexToRgba(color, 0));

    ctx.save();
    ctx.fillStyle = glow;
    ctx.filter = "blur(18px)";
    ctx.globalAlpha = isFocused ? 1 : 0.08;
    ctx.fill(territory.path);
    ctx.restore();

    ctx.save();
    ctx.fillStyle = hexToRgba(color, territory.count < 8 ? 0.12 : 0.08);
    ctx.strokeStyle = hexToRgba(color, territory.count < 8 ? 0.34 : 0.22);
    ctx.lineWidth = territory.count < 8 ? 1.8 : 1.1;
    ctx.setLineDash(territory.count < 8 ? [5, 5] : []);
    ctx.globalAlpha = isFocused ? 1 : 0.08;
    ctx.fill(territory.path);
    ctx.stroke(territory.path);
    ctx.restore();
  });
}

function drawScene() {
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  ctx.clearRect(0, 0, width, height);

  const gradient = ctx.createRadialGradient(width * 0.5, height * 0.4, 40, width * 0.5, height * 0.5, width * 0.7);
  gradient.addColorStop(0, "rgba(25, 64, 79, 0.45)");
  gradient.addColorStop(1, "rgba(4, 11, 17, 0.04)");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, width, height);

  if (!state.rows.length) {
    emptyStateEl.classList.remove("hidden");
    return;
  }

  emptyStateEl.classList.add("hidden");
  projectPoints();
  buildClusterTerritories();
  drawAxes();
  drawClusterTerritories();

  let selectedPoint = null;
  let neighborhood = new Map();
  const passiveAlbumArtIndices = new Set(
    state.projectedPoints
      .filter((point) => point.row.album_image_url && point.depth < 1.35)
      .sort((a, b) => a.depth - b.depth)
      .slice(0, 36)
      .map((point) => point.index)
  );
  if (state.selectedIndex !== -1) {
    selectedPoint = state.projectedPoints.find((point) => point.index === state.selectedIndex) || null;
    const selectedCoord = state.normalizedCoords.find((point) => point.index === state.selectedIndex);
    if (selectedCoord) {
      const selectedCluster = String(state.rows[state.selectedIndex]?.cluster ?? "unclustered");
      neighborhood = new Map(
        state.normalizedCoords.map((coord) => {
          const sameCluster = String(coord.row.cluster ?? "unclustered") === selectedCluster;
          const dx = coord.x - selectedCoord.x;
          const dy = coord.y - selectedCoord.y;
          const dz = coord.z - selectedCoord.z;
          const distance = Math.sqrt(dx * dx + dy * dy + dz * dz);
          const proximity = sameCluster ? Math.max(0, 1 - distance / 0.75) : 0;
          return [coord.index, proximity];
        })
      );
    }
  }

  state.projectedPoints.forEach((point) => {
    const cluster = String(point.row.cluster ?? "unclustered");
    const focused = state.clusterValue === "All clusters" || state.clusterValue === cluster;
    const baseFill = state.colors.get(cluster) || "#ffffff";
    const fill = focused ? baseFill : desaturateColor(baseFill, 0.92);
    const proximity = neighborhood.get(point.index) || 0;
    const selectedCluster = state.selectedIndex !== -1
      ? String(state.rows[state.selectedIndex]?.cluster ?? "unclustered")
      : null;
    const inSelectedCluster = selectedCluster !== null && cluster === selectedCluster;
    ctx.beginPath();
    ctx.fillStyle = fill;
    const baseAlpha = point.index === state.hoveredIndex ? 1 : focused ? 0.9 : 0.035;
    const alpha = selectedPoint ? Math.max(baseAlpha * (focused ? 0.2 : 0.08), proximity * 0.95, point.index === state.selectedIndex ? 1 : 0) : baseAlpha;
    ctx.globalAlpha = alpha;
    const depthFieldScale = selectedPoint && inSelectedCluster && point.index !== state.selectedIndex
      ? 0.45 + proximity * 0.95
      : 1;
    const radius = (point.size + proximity * 1.6) * depthFieldScale;
    const showAlbumArt = Boolean(
      point.row.album_image_url &&
      ((selectedPoint && inSelectedCluster) || passiveAlbumArtIndices.has(point.index))
    );
    const albumArtRadius = (selectedPoint && inSelectedCluster) || passiveAlbumArtIndices.has(point.index)
      ? Math.max(radius, 6)
      : radius;

    if (showAlbumArt) {
      drawAlbumCoverPoint(point, fill, albumArtRadius);
    } else {
      ctx.arc(point.screenX, point.screenY, radius, 0, Math.PI * 2);
      ctx.fill();
    }

    if (selectedPoint && inSelectedCluster && point.index !== state.selectedIndex && proximity < 0.55) {
      ctx.beginPath();
      ctx.fillStyle = hexToRgba(fill, Math.max(0.04, 0.18 * (1 - proximity)));
      ctx.globalAlpha = Math.max(0.08, 0.3 * (1 - proximity));
      ctx.arc(point.screenX, point.screenY, radius * (2.2 - proximity), 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = fill;
      ctx.globalAlpha = alpha;
    }

    if (point.index === state.selectedIndex) {
      ctx.strokeStyle = "#ffffff";
      ctx.lineWidth = 2;
      ctx.stroke();
    } else if (proximity > 0.18) {
      ctx.strokeStyle = hexToRgba(fill, Math.min(0.8, proximity * 0.7));
      ctx.lineWidth = 1;
      ctx.stroke();
    }
  });

  if (selectedPoint) {
    state.projectedPoints
      .filter((point) => point.index !== state.selectedIndex && (neighborhood.get(point.index) || 0) > 0.24)
      .sort((a, b) => (neighborhood.get(b.index) || 0) - (neighborhood.get(a.index) || 0))
      .slice(0, 20)
      .forEach((point) => {
        const proximity = neighborhood.get(point.index) || 0;
        ctx.save();
        ctx.strokeStyle = hexToRgba(state.colors.get(String(point.row.cluster ?? "unclustered")) || "#ffffff", proximity * 0.35);
        ctx.lineWidth = 1 + proximity * 0.6;
        ctx.setLineDash([2, 6]);
        ctx.beginPath();
        ctx.moveTo(selectedPoint.screenX, selectedPoint.screenY);
        ctx.lineTo(point.screenX, point.screenY);
        ctx.stroke();
        ctx.restore();
      });

    const glow = ctx.createRadialGradient(
      selectedPoint.screenX,
      selectedPoint.screenY,
      0,
      selectedPoint.screenX,
      selectedPoint.screenY,
      90
    );
    glow.addColorStop(0, "rgba(255,255,255,0.18)");
    glow.addColorStop(0.5, "rgba(140,246,255,0.08)");
    glow.addColorStop(1, "rgba(140,246,255,0)");
    ctx.save();
    ctx.fillStyle = glow;
    ctx.beginPath();
    ctx.arc(selectedPoint.screenX, selectedPoint.screenY, 90, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  ctx.globalAlpha = 1;

  if (state.hoveredIndex !== -1) {
    renderTooltip(state.hoveredIndex);
  } else {
    tooltipEl.classList.add("hidden");
  }
}

function findNearestPoint(clientX, clientY) {
  const rect = canvas.getBoundingClientRect();
  const x = clientX - rect.left;
  const y = clientY - rect.top;
  let best = null;

  state.projectedPoints.forEach((point) => {
    const cluster = String(point.row.cluster ?? "unclustered");
    if (state.clusterValue !== "All clusters" && cluster !== state.clusterValue) {
      return;
    }
    const dx = point.screenX - x;
    const dy = point.screenY - y;
    const distance = Math.sqrt(dx * dx + dy * dy);
    if (distance <= Math.max(point.size + 4, 8)) {
      if (!best || distance < best.distance) {
        best = { index: point.index, distance, screenX: point.screenX, screenY: point.screenY };
      }
    }
  });

  return best;
}

function renderTooltip(rowIndex) {
  const row = state.rows[rowIndex];
  const point = state.projectedPoints.find((entry) => entry.index === rowIndex);
  if (!row || !point) {
    tooltipEl.classList.add("hidden");
    return;
  }

  tooltipEl.innerHTML = `
    <strong>${escapeHtml(row.track_name || "Unknown track")}</strong><br>
    <span>${escapeHtml(row.artist_names || "Unknown artist")}</span><br>
    <span>Cluster ${escapeHtml(String(row.cluster ?? "n/a"))}</span><br>
    ${state.algorithmColumn ? `<span>${escapeHtml(row[state.algorithmColumn] || "")}</span><br>` : ""}
    <span>${state.axis.x}: ${formatValue(row[state.axis.x])}</span><br>
    <span>${state.axis.y}: ${formatValue(row[state.axis.y])}</span><br>
    <span>${state.axis.z}: ${formatValue(row[state.axis.z])}</span>
  `;
  tooltipEl.style.left = `${Math.min(point.screenX + 18, canvas.clientWidth - 240)}px`;
  tooltipEl.style.top = `${Math.max(point.screenY - 24, 16)}px`;
  tooltipEl.classList.remove("hidden");
}

function renderDetailCard() {
  if (state.selectedIndex === -1) {
    detailCardEl.classList.add("hidden");
    return;
  }

  const row = state.rows[state.selectedIndex];
  if (!row) {
    detailCardEl.classList.add("hidden");
    return;
  }

  detailCardEl.innerHTML = `
    <h3>${escapeHtml(row.track_name || "Unknown track")}</h3>
    <p>${escapeHtml(row.artist_names || "Unknown artist")}</p>
    <p>Album: ${escapeHtml(row.album_name || "Unknown album")}</p>
    <p>Cluster: ${escapeHtml(String(row.cluster ?? "n/a"))}</p>
    <p>${escapeHtml(state.axis.x)}: ${formatValue(row[state.axis.x])}</p>
    <p>${escapeHtml(state.axis.y)}: ${formatValue(row[state.axis.y])}</p>
    <p>${escapeHtml(state.axis.z)}: ${formatValue(row[state.axis.z])}</p>
    ${state.algorithmColumn ? `<p>Algorithm: ${escapeHtml(row[state.algorithmColumn] || "n/a")}</p>` : ""}
  `;
  detailCardEl.classList.remove("hidden");
}

function focusOnSong(rowIndex) {
  const row = state.rows[rowIndex];
  const coord = state.normalizedCoords.find((point) => point.index === rowIndex);
  if (!row || !coord) {
    return;
  }

  state.selectedIndex = rowIndex;
  state.clusterValue = String(row.cluster ?? "unclustered");
  clusterFilter.value = state.clusterValue;
  state.focusTarget = {
    ...state.focusTarget,
  };
  state.targetFocusTarget = {
    x: coord.x,
    y: coord.y,
    z: coord.z,
  };
  state.targetDistance = 0.22;
  updateStats();
  renderDetailCard();
  renderClusterDetail();
  setClearSongFocusVisibility();
}

function renderClusterDetail() {
  if (state.clusterValue === "All clusters") {
    clusterDetailEl.innerHTML = `<p class="hint">Choose a cluster to isolate it, inspect its songs, and navigate its local territory.</p>`;
    return;
  }

  const rows = state.rows.filter((row) => String(row.cluster) === state.clusterValue);
  const sample = rows
    .slice(0, 5)
    .map((row) => `${escapeHtml(row.track_name || "Unknown track")} by ${escapeHtml(row.artist_names || "Unknown artist")}`)
    .join("<br>");

  clusterDetailEl.innerHTML = `
    <p><strong>Cluster ${escapeHtml(state.clusterValue)}</strong></p>
    <p>${rows.length} visible songs in this cluster.</p>
    <p>Current axes: ${escapeHtml(state.axis.x)}, ${escapeHtml(state.axis.y)}, ${escapeHtml(state.axis.z)}</p>
    <p>Sample songs:<br>${sample || "No songs available."}</p>
  `;
}

function formatValue(value) {
  return Number.isFinite(value) ? value.toFixed(2) : String(value ?? "");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function updateFromControls() {
  state.axis = {
    x: xAxisSelect.value,
    y: yAxisSelect.value,
    z: zAxisSelect.value,
  };
  refreshClusterFilter();
  state.clusterValue = clusterFilter.value || state.clusterValue;
  state.hoveredIndex = -1;
  state.selectedIndex = -1;
  state.targetFocusTarget = { x: 0, y: 0, z: 0 };
  state.targetDistance = 3.2;
  filterRows();
  updateStats();
  renderDetailCard();
  renderClusterDetail();
  requestDraw();
}

async function handleFileUpload(event) {
  await loadSelectedClusteringFile();
}

async function handlePlaylistFileUpload(event) {
  await loadDefaultPlaylistDataset();
}

if (playlistMenuEl) {
  playlistMenuEl.addEventListener("click", (event) => {
    const button = event.target.closest("[data-mood], [data-track-id], [data-action]");
    if (!button) {
      return;
    }

    if (button.dataset.mood) {
      startPlaylistBuilder(button.dataset.mood);
      return;
    }

    if (button.dataset.trackId) {
      choosePlaylistSong(button.dataset.trackId);
      return;
    }

    if (button.dataset.action === "generate-playlist") {
      addDebugLog("playlist:generate-click");
      const lengthInput = document.getElementById("playlistLengthInput");
      const requestedLength = Number(lengthInput?.value || state.playlistBuilder.playlistLength || 20);
      const playlistLength = Math.max(5, Math.min(100, requestedLength));
      state.playlistBuilder = {
        ...state.playlistBuilder,
        playlistLength,
        isGenerating: true,
      };
      window.requestAnimationFrame(() => {
        renderPlaylistBuilder();
        addDebugLog("playlist:rendered");
        window.setTimeout(() => {
          addDebugLog(`playlist:generate-run length=${playlistLength}`);
          buildGeneratedPlaylist(
            state.playlistBuilder.mood,
            state.playlistBuilder.picks,
            playlistLength,
          )
            .then((playlist) => {
              addDebugLog("playlist:built");
              state.playlistBuilder = {
                ...state.playlistBuilder,
                playlistLength,
                isGenerating: false,
                generatedPlaylist: playlist,
                error: "",
              };
              window.requestAnimationFrame(() => {
                renderPlaylistBuilder();
                addDebugLog(`playlist:generate-finished total=${state.playlistBuilder.generatedPlaylist.length}`);
              });
            })
            .catch((error) => {
              addDebugLog(`playlist:generate-error ${error.message}`);
              state.playlistBuilder = {
                ...state.playlistBuilder,
                playlistLength,
                isGenerating: false,
                error: error.message,
              };
              window.requestAnimationFrame(() => {
                renderPlaylistBuilder();
              });
            });
        }, 0);
      });

      return;
    }

    if (button.dataset.action === "restart-playlist") {
      state.playlistBuilder = {
        mood: null,
        step: 0,
        picks: [],
        candidates: [],
        playlistLength: 20,
        generatedPlaylist: [],
        isGenerating: false,
        error: "",
      };
      renderPlaylistBuilder();
    }
  });
}

[xAxisSelect, yAxisSelect, zAxisSelect, clusterFilter]
  .filter(Boolean)
  .forEach((element) => {
    element.addEventListener("change", updateFromControls);
  });

[clusterMethodSelect, clusterKInput, clusterEpsInput, clusterMinSamplesInput]
  .filter(Boolean)
  .forEach((element) => {
    element.addEventListener("input", () => {
      syncClusteringFileControls();
    });
    element.addEventListener("change", async () => {
      try {
        await loadSelectedClusteringFile();
      } catch (error) {
        addDebugLog(`loadSelectedClusteringFile:error ${error.message}`);
      }
    });
  });

if (canvas && pageMode === "clustering") {
  canvas.addEventListener("pointerdown", (event) => {
    state.dragging = true;
    state.pointerMoved = false;
    state.lastPointer = { x: event.clientX, y: event.clientY };
    canvas.setPointerCapture(event.pointerId);
  });

  canvas.addEventListener("pointermove", (event) => {
    if (state.dragging) {
      const dx = event.clientX - state.lastPointer.x;
      const dy = event.clientY - state.lastPointer.y;
      if (Math.abs(dx) + Math.abs(dy) > 2) {
        state.pointerMoved = true;
      }
      state.yaw += dx * 0.01;
      state.pitch = Math.max(-1.45, Math.min(1.45, state.pitch + dy * 0.01));
      state.lastPointer = { x: event.clientX, y: event.clientY };
      requestDraw();
      return;
    }

    const nearest = findNearestPoint(event.clientX, event.clientY);
    state.hoveredIndex = nearest ? nearest.index : -1;
    requestDraw();
  });

  canvas.addEventListener("pointerup", (event) => {
    if (!state.pointerMoved) {
      const nearest = findNearestPoint(event.clientX, event.clientY);
      if (nearest) {
        focusOnSong(nearest.index);
      } else {
        state.selectedIndex = -1;
        state.targetFocusTarget = { x: 0, y: 0, z: 0 };
        state.targetDistance = 3.2;
        renderDetailCard();
        renderClusterDetail();
      }
      requestDraw();
    }
    state.dragging = false;
    state.pointerMoved = false;
    canvas.releasePointerCapture(event.pointerId);
  });

  canvas.addEventListener("pointerleave", () => {
    if (!state.dragging) {
      state.hoveredIndex = -1;
      if (tooltipEl) {
        tooltipEl.classList.add("hidden");
      }
      requestDraw();
    }
  });

  canvas.addEventListener("wheel", (event) => {
    event.preventDefault();
    state.targetDistance = Math.max(0.12, Math.min(28, state.targetDistance + event.deltaY * 0.005));
    requestDraw();
  }, { passive: false });
}

if (resetCameraBtn) {
  resetCameraBtn.addEventListener("click", () => {
    state.yaw = -0.65;
    state.pitch = 0.35;
    state.targetDistance = 3.2;
    state.targetFocusTarget = { x: 0, y: 0, z: 0 };
    state.selectedIndex = -1;
    state.clusterValue = "All clusters";
    if (clusterFilter) {
      clusterFilter.value = state.clusterValue;
    }
    updateStats();
    renderDetailCard();
    renderClusterDetail();
    requestDraw();
  });
}

if (clearSongFocusBtn) {
  clearSongFocusBtn.addEventListener("click", () => {
    state.selectedIndex = -1;
    state.targetDistance = 3.2;
    state.targetFocusTarget = { x: 0, y: 0, z: 0 };
    renderDetailCard();
    renderClusterDetail();
    requestDraw();
  });
}

if (pageMode === "clustering") {
  window.addEventListener("resize", resizeCanvas);
  renderPlaylistBuilder();
  applyActiveTab();
  resizeCanvas();
  syncClusteringFileControls();
  loadSelectedClusteringFile().catch((error) => {
    addDebugLog(`loadSelectedClusteringFile:error ${error.message}`);
  });
} else {
  state.activeTab = "playlist";
  renderPlaylistBuilder();
  applyActiveTab();
  loadDefaultPlaylistDataset().catch((error) => {
    addDebugLog(`loadDefaultPlaylistDataset:error ${error.message}`);
    state.playlistBuilder = {
      ...state.playlistBuilder,
      error: "Unable to load clustering/all_tracks.csv. Import music first.",
    };
    renderPlaylistBuilder();
  });
}