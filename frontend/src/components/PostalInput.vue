<script setup>
import { ref, computed } from 'vue'

const props = defineProps({
  modelValue: { type: String, default: '' }
})
const emit = defineEmits(['update:modelValue', 'confirmed'])

const input = ref(props.modelValue)
const touched = ref(false)

// Canadian postal code: first letter A-V (no D, F, I, O, Q, U, W)
// But we keep validation loose — just format check
const POSTAL_RE = /^[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z]\s?\d[ABCEGHJ-NPRSTV-Z]\d$/i

const isValid = computed(() => POSTAL_RE.test(input.value.trim()))
const showError = computed(() => touched.value && input.value.length >= 3 && !isValid.value)
const confirmed = ref(false)

function onInput(e) {
  input.value = e.target.value
  emit('update:modelValue', input.value)
  confirmed.value = false
}

function onBlur() {
  touched.value = true
}

function onSubmit() {
  touched.value = true
  if (!isValid.value) return
  confirmed.value = true
  emit('confirmed', input.value.trim().toUpperCase())
}

function onKeydown(e) {
  if (e.key === 'Enter') onSubmit()
}
</script>

<template>
  <div class="postal-input" :class="{ confirmed }">
    <label class="pi-label" for="postal">Your Postal Code</label>
    <div class="pi-row">
      <input
        id="postal"
        class="pi-input"
        :class="{ error: showError, valid: isValid && touched }"
        type="text"
        maxlength="7"
        placeholder="M5V 1J5"
        autocomplete="postal-code"
        :value="input"
        @input="onInput"
        @blur="onBlur"
        @keydown="onKeydown"
      />
      <button class="pi-btn" :disabled="!isValid" @click="onSubmit" aria-label="Find stores">
        <span v-if="!confirmed">Find Stores</span>
        <span v-else>✓</span>
      </button>
    </div>
    <p v-if="showError" class="pi-error">
      Please enter a valid Canadian postal code (e.g. M5V 2H1)
    </p>
    <p v-else-if="confirmed" class="pi-hint confirmed-hint">
      Showing stores near {{ input.trim().toUpperCase() }}
    </p>
    <p v-else class="pi-hint">Enter your postal code to see stores near you</p>
  </div>
</template>

<style scoped>
.postal-input {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.pi-label {
  font-family: var(--font-display);
  font-size: 0.8rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--c-amber);
}

.pi-row {
  display: flex;
  gap: 0;
}

.pi-input {
  flex: 1;
  background: var(--c-surface);
  border: 1px solid var(--c-border);
  border-right: none;
  color: var(--c-ivory);
  font-family: var(--font-body);
  font-size: 1.1rem;
  letter-spacing: 0.12em;
  padding: 12px 16px;
  outline: none;
  text-transform: uppercase;
  transition: border-color 0.2s;
  border-radius: 3px 0 0 3px;
}
.pi-input::placeholder {
  color: var(--c-muted);
  text-transform: none;
}
.pi-input:focus {
  border-color: var(--c-amber);
}
.pi-input.error {
  border-color: #c0392b;
}
.pi-input.valid {
  border-color: #27ae60;
}

.pi-btn {
  background: var(--c-amber);
  border: 1px solid var(--c-amber);
  color: var(--c-bg);
  font-family: var(--font-display);
  font-weight: 700;
  font-size: 0.85rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 0 20px;
  cursor: pointer;
  border-radius: 0 3px 3px 0;
  transition: background 0.2s, opacity 0.2s;
  white-space: nowrap;
}
.pi-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
.pi-btn:not(:disabled):hover {
  background: #d4900a;
  border-color: #d4900a;
}

.pi-error {
  font-family: var(--font-body);
  font-size: 0.75rem;
  color: #e74c3c;
  margin: 0;
}

.pi-hint {
  font-family: var(--font-body);
  font-size: 0.75rem;
  color: var(--c-muted);
  margin: 0;
  font-style: italic;
}
.confirmed-hint {
  color: #27ae60;
  font-style: normal;
}
</style>
