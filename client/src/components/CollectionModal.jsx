import React, { useState, useEffect, useRef } from "react";
import { collectionsApi } from "../api/collectionsApi";
import { useToast } from "../contexts/ToastContext";

const CollectionModal = ({ isOpen, onClose, onSave, collection = null, customer, mode = "create" }) => {
  const [formData, setFormData] = useState({
    name: "",
    description: "",
    customer_id: null
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // All hooks must always run — no condition above this line
  const { showSuccess, showError } = useToast();
  const nameRef = useRef(null);
  const modalRef = useRef(null);
  const prevActiveRef = useRef(null);

  // Initialize modal form data when opened
  useEffect(() => {
    if (!isOpen) return;

    if (mode === "edit" && collection) {
      setFormData({
        name: collection.name || "",
        description: collection.description || "",
        customer_id: collection.customer_id
      });
    } else {
      setFormData({
        name: "",
        description: "",
        customer_id: customer ? customer.id : null
      });
    }

    setError("");
  }, [isOpen, mode, collection, customer]);

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!formData.name.trim()) {
      setError("Collection name is required");
      return;
    }

    setLoading(true);
    setError("");

    try {
      if (mode === "create") {
        await collectionsApi.create(formData);
        showSuccess("Collection created successfully");
      } else {
        await collectionsApi.update(collection.id, formData);
        showSuccess("Collection updated successfully");
      }

      onSave();
      onClose();
    } catch (err) {
      const msg = err.response?.data?.error || "Failed to save collection";
      setError(msg);
      showError(msg);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (e) => {
    setFormData((prev) => ({
      ...prev,
      [e.target.name]: e.target.value
    }));
  };

  // Focus trapping and autofocus
  useEffect(() => {
    if (!isOpen) {
      try { prevActiveRef.current?.focus(); } catch {}
      return;
    }

    prevActiveRef.current = document.activeElement;

    setTimeout(() => {
      nameRef.current?.focus();
    }, 50);
  }, [isOpen]);

  useEffect(() => {
    const trap = (e) => {
      if (!isOpen) return;

      if (e.key === "Escape") onClose();

      if (e.key === "Tab") {
        const focusable = modalRef.current?.querySelectorAll(
          'a,button,input,select,textarea,[tabindex]:not([tabindex="-1"])'
        );
        if (!focusable?.length) return;

        const list = Array.from(focusable);
        const index = list.indexOf(document.activeElement);

        if (e.shiftKey && index === 0) {
          e.preventDefault();
          list[list.length - 1].focus();
        } else if (!e.shiftKey && index === list.length - 1) {
          e.preventDefault();
          list[0].focus();
        }
      }
    };

    window.addEventListener("keydown", trap);
    return () => window.removeEventListener("keydown", trap);
  }, [isOpen, onClose]);

  // *** IMPORTANT FIX ***
  // No early return before hooks — now we conditionally render UI inside return
  return (
    isOpen && (
      <div className="fixed inset-0 bg-black bg-opacity-50 dark:bg-black dark:bg-opacity-70 flex items-center justify-center z-50">
        <div ref={modalRef} role="dialog" aria-modal="true" className="bg-white dark:bg-slate-800 rounded-lg p-6 w-full max-w-md mx-4 text-slate-900 dark:text-slate-100">
          <h2 className="text-xl font-semibold mb-4">
            {mode === "create" ? "Create New Collection" : "Edit Collection"}
          </h2>

          <form onSubmit={handleSubmit}>
            <div className="mb-4">
              <label className="block text-sm font-medium mb-2">Collection Name *</label>
              <input
                ref={nameRef}
                type="text"
                name="name"
                value={formData.name}
                onChange={handleChange}
                className="w-full px-3 py-2 border rounded-md bg-white dark:bg-slate-700 text-slate-900 dark:text-slate-100 focus:outline-none"
                required
              />
            </div>

            <div className="mb-4">
              <label className="block text-sm font-medium mb-2">Description</label>
              <textarea
                name="description"
                rows={3}
                value={formData.description}
                onChange={handleChange}
                className="w-full px-3 py-2 border rounded-md bg-white dark:bg-slate-700 text-slate-900 dark:text-slate-100 focus:outline-none"
              />
            </div>

            {error && <div className="mb-4 text-red-500">{error}</div>}

            <div className="flex justify-end space-x-3">
              <button type="button" onClick={onClose} disabled={loading} className="px-4 py-2 bg-gray-200 rounded-md dark:bg-slate-700">
                Cancel
              </button>
              <button type="submit" disabled={loading} className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
                {loading ? "Saving..." : mode === "create" ? "Create" : "Update"}
              </button>
            </div>
          </form>
        </div>
      </div>
    )
  );
};

export default CollectionModal;
