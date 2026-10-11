"use strict";

/**
 * ECLIPSE Human Typing
 * --------------------
 * Owns only the Messenger typing-indicator lifecycle. It deliberately has
 * no text-generation or delay-calculation logic.
 */

function startTyping(api, threadID) {
  if (!api || !threadID) return false;
  try {
    if (typeof api.sendTypingIndicator === "function") {
      api.sendTypingIndicator(threadID, (error) => {
        if (error) {
          console.warn("[HUMAN-TYPING] Typing indicator failed:", error?.message || error);
        }
      });
      return true;
    }
    if (typeof api.sendTyping === "function") {
      api.sendTyping(threadID, true);
      return true;
    }
  } catch (error) {
    console.warn("[HUMAN-TYPING] Unable to start typing indicator:", error?.message || error);
  }
  return false;
}

function stopTyping(api, threadID) {
  if (!api || !threadID) return false;
  try {
    if (typeof api.sendTyping === "function") {
      api.sendTyping(threadID, false);
      return true;
    }
  } catch (error) {
    console.warn("[HUMAN-TYPING] Unable to stop typing indicator:", error?.message || error);
  }
  return false;
}

function withTyping(api, threadID, durationMs, send) {
  const duration = Math.max(0, Number(durationMs) || 0);
  startTyping(api, threadID);

  return new Promise((resolve, reject) => {
    setTimeout(async () => {
      try {
        const result = await send();
        stopTyping(api, threadID);
        resolve(result);
      } catch (error) {
        stopTyping(api, threadID);
        reject(error);
      }
    }, duration);
  });
}

module.exports = {
  startTyping,
  stopTyping,
  withTyping,
};
