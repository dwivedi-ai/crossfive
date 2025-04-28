// stereotype_quiz_app/static/js/script.js

document.addEventListener('DOMContentLoaded', function() {

    // --- Constants ---
    const MAX_SELECTED_STATES = 5; // The required number of target states

    // --- DOM Element References ---
    // Index Page Elements
    const userInfoForm = document.getElementById('user-info-form');
    const nativeStateSelect = document.getElementById('native_state');
    const stateCheckboxesContainer = document.getElementById('state-checkboxes');
    const stateCheckboxes = stateCheckboxesContainer ? stateCheckboxesContainer.querySelectorAll('input[type="checkbox"][name="selected_states"]') : [];
    const selectedCountFeedback = document.getElementById('selected-count-feedback');
    const stateSelectionError = document.getElementById('state-selection-error');
    const startQuizButton = document.getElementById('start-quiz-button');

    // Quiz Page Elements
    const quizForm = document.getElementById('quiz-form');
    const toggleButtons = document.querySelectorAll('.toggle-subsets');
    const annotationFieldsets = document.querySelectorAll('.annotation-options');


    // --- INDEX PAGE LOGIC (State Selection) ---

    // Function to update state checkbox states based on native selection and count
    const updateStateSelection = () => {
        // Ensure necessary elements exist on the index page
        if (!nativeStateSelect || !stateCheckboxes.length || !selectedCountFeedback) return;

        const selectedNativeState = nativeStateSelect.value;
        let currentSelectedCount = 0;

        // First pass: Enable all, disable native, count selected
        stateCheckboxes.forEach(checkbox => {
            const parentDiv = checkbox.closest('.state-checkbox-item'); // Get parent for styling

            // Reset disabled state and appearance first
            checkbox.disabled = false;
            if (parentDiv) {
                parentDiv.style.opacity = '1';
                parentDiv.style.backgroundColor = ''; // Reset background
                parentDiv.classList.remove('disabled-item'); // Remove helper class if exists
            }

            // Check if it's the native state
            if (checkbox.value === selectedNativeState && selectedNativeState !== "") {
                checkbox.checked = false; // Uncheck if it was selected
                checkbox.disabled = true;
                 if (parentDiv) {
                     parentDiv.style.opacity = '0.6'; // Visually indicate disabled
                     parentDiv.style.backgroundColor = '#f8f8f8'; // Subtle background
                     parentDiv.classList.add('disabled-item');
                 }
            }

            // Count checked checkboxes (excluding the potentially disabled native state)
            if (checkbox.checked && !checkbox.disabled) {
                currentSelectedCount++;
            }
        });

        // Update selected count feedback
        selectedCountFeedback.textContent = `Selected: ${currentSelectedCount} / ${MAX_SELECTED_STATES}`;
        selectedCountFeedback.style.color = (currentSelectedCount === MAX_SELECTED_STATES) ? 'green' : '#555'; // Green when correct count

        // Second pass: Disable remaining checkboxes if max count is reached
        if (currentSelectedCount >= MAX_SELECTED_STATES) {
            stateCheckboxes.forEach(checkbox => {
                const parentDiv = checkbox.closest('.state-checkbox-item');
                // Disable only those that are NOT checked and NOT already disabled (i.e., not the native state)
                if (!checkbox.checked && !checkbox.disabled) {
                    checkbox.disabled = true;
                     if (parentDiv) {
                         parentDiv.style.opacity = '0.6';
                         parentDiv.style.backgroundColor = '#f8f8f8';
                          parentDiv.classList.add('disabled-item');
                     }
                }
            });
        }

        // Update submit button state
        if (startQuizButton) {
            const isCorrectCount = (currentSelectedCount === MAX_SELECTED_STATES);
            startQuizButton.disabled = !isCorrectCount;
            startQuizButton.title = isCorrectCount ? 'Start the quiz' : `Please select exactly ${MAX_SELECTED_STATES} states`;
            // Visual cue for button state
            startQuizButton.style.opacity = isCorrectCount ? '1' : '0.6';
        }

        // Hide general count error message initially; submit validation handles specific errors
        if (stateSelectionError) {
             stateSelectionError.style.display = 'none';
        }
    };

    // Function to validate state selection on submit (client-side check)
    const validateStateSelectionSubmit = (event) => {
        // Ensure we are on the index page and elements exist
        if (!nativeStateSelect || !stateCheckboxes.length || !stateSelectionError) return true; // Allow submission if elements missing (shouldn't happen)

        const selectedNativeState = nativeStateSelect.value;
        const checkedStates = Array.from(stateCheckboxes).filter(cb => cb.checked);
        const selectedCount = checkedStates.length;
        let isValid = true;
        let errorMessage = "";

        // Check 1: Correct number of states selected
        if (selectedCount !== MAX_SELECTED_STATES) {
            isValid = false;
            errorMessage = `Please select exactly ${MAX_SELECTED_STATES} states/UTs. You selected ${selectedCount}.`;
        }
        // Check 2: Native state is not selected (redundant if UI logic is correct, but safe)
        else if (selectedNativeState && checkedStates.some(cb => cb.value === selectedNativeState)) {
            isValid = false;
            errorMessage = "Your native state cannot be selected as a target state. Please uncheck it.";
        }

        if (!isValid) {
            if (event) event.preventDefault(); // Stop the form submission
            stateSelectionError.textContent = errorMessage;
            stateSelectionError.style.display = 'block'; // Show error message
            // Scroll to the error message for visibility
            stateSelectionError.scrollIntoView({ behavior: 'smooth', block: 'center' });
            return false; // Indicate validation failure
        } else {
            stateSelectionError.style.display = 'none'; // Hide error if valid
            return true; // Indicate validation success
        }
    };


    // Add event listeners for Index Page elements IF they exist
    if (userInfoForm) {
        // Initial setup on page load for the index page
        updateStateSelection();

        if (nativeStateSelect) {
            nativeStateSelect.addEventListener('change', updateStateSelection);
        }

        if (stateCheckboxesContainer) {
            // Use event delegation for checkboxes
            stateCheckboxesContainer.addEventListener('change', (event) => {
                 if (event.target.type === 'checkbox' && event.target.name === 'selected_states') {
                     updateStateSelection(); // Update counts and disabled states on any checkbox change
                 }
            });
        }

        // Add validation on form submit
        userInfoForm.addEventListener('submit', validateStateSelectionSubmit);
    }


    // --- QUIZ PAGE LOGIC ---

    // --- Toggle Subset Visibility ---
    toggleButtons.forEach(button => {
        button.addEventListener('click', function() {
            const targetId = this.getAttribute('data-target');
            const targetElement = document.getElementById(targetId);
            if (targetElement) {
                const isHidden = targetElement.style.display === 'none' || targetElement.style.display === '';
                targetElement.style.display = isHidden ? 'block' : 'none';
                this.textContent = isHidden ? 'Hide Details' : 'Show Details';
                // Update ARIA attributes for accessibility
                this.setAttribute('aria-expanded', isHidden ? 'true' : 'false');
                targetElement.setAttribute('aria-hidden', isHidden ? 'false' : 'true');
            } else {
                console.error('Could not find subset target element with ID:', targetId);
            }
        });
    });


    // --- Show/Hide Offensiveness Rating Based on Annotation ---
    annotationFieldsets.forEach(fieldset => {
        // Find the elements within this specific fieldset context
        const ratingContainer = document.getElementById(`rating_container_${fieldset.getAttribute('data-question-index')}`);
        if (!ratingContainer) {
            // If the rating container doesn't exist for this item, skip adding listeners
             // console.warn(`Rating container not found for index: ${fieldset.getAttribute('data-question-index')}`);
             return;
        }
        const ratingRadios = ratingContainer.querySelectorAll('input[type="radio"]');
        const ratingLegendAsterisk = ratingContainer.querySelector('legend .required-indicator'); // Target the asterisk

        const handleAnnotationChange = (targetRadio) => {
            // Ensure it's a radio button within this fieldset that triggered the change
            if (!targetRadio || targetRadio.type !== 'radio' || !fieldset.contains(targetRadio)) return;

            const selectedValue = targetRadio.value;

            // Only act if the rating container was found
            if (selectedValue === 'Stereotype' && targetRadio.checked) {
                ratingContainer.style.display = 'block'; // Show container
                ratingRadios.forEach(radio => radio.required = true); // Make rating radios required
                if (ratingLegendAsterisk) ratingLegendAsterisk.style.display = 'inline'; // Show asterisk
            } else {
                ratingContainer.style.display = 'none'; // Hide container
                ratingRadios.forEach(radio => {
                    radio.required = false; // Make not required
                    radio.checked = false; // IMPORTANT: Uncheck any selected rating if hiding
                });
                 if (ratingLegendAsterisk) ratingLegendAsterisk.style.display = 'none'; // Hide asterisk
            }
        };

        // Listen for changes within the annotation fieldset
        fieldset.addEventListener('change', function(event) {
            handleAnnotationChange(event.target);
        });

        // Initial check on page load for pre-selected values (e.g., if user uses back button)
        const checkedAnnotationRadio = fieldset.querySelector('input[type="radio"]:checked');
        if (checkedAnnotationRadio) {
            handleAnnotationChange(checkedAnnotationRadio); // Apply logic based on initial state
        } else {
             // Ensure rating container is hidden and not required initially if nothing is checked
             ratingContainer.style.display = 'none';
             ratingRadios.forEach(radio => radio.required = false);
             if (ratingLegendAsterisk) ratingLegendAsterisk.style.display = 'none';
        }
    });


    // --- Enhanced Client-side Form Validation on Quiz Submit ---
     if (quizForm) {
        quizForm.addEventListener('submit', function(event) {
            let firstErrorElement = null;
            let validationPassed = true;

            // Clear previous visual error highlights
            document.querySelectorAll('.validation-error-highlight').forEach(el => {
                 el.classList.remove('validation-error-highlight');
                 // Reset border/outline styles that might have been added directly
                 el.style.borderColor = '';
                 el.style.outline = '';
            });
            // Also clear direct styles if they were applied to fieldsets/containers
             quizForm.querySelectorAll('fieldset[style*="border-color: red"], div[style*="border-color: red"]').forEach(el => {
                 el.style.borderColor = '';
             });


            // 1. Check Familiarity Rating (Required)
            const familiarityRadios = quizForm.querySelectorAll('input[name="familiarity_rating"]');
            if (familiarityRadios.length > 0) { // Check if the element exists on the page
                const familiaritySelected = Array.from(familiarityRadios).some(radio => radio.checked);
                const familiarityFieldset = familiarityRadios.length > 0 ? familiarityRadios[0].closest('fieldset') : null;

                if (!familiaritySelected) {
                    console.warn("Validation Fail: Familiarity rating missing.");
                    validationPassed = false;
                    if (familiarityFieldset) {
                         if (!firstErrorElement) firstErrorElement = familiarityFieldset;
                         familiarityFieldset.classList.add('validation-error-highlight');
                         // familiarityFieldset.style.borderColor = 'red'; // CSS class should handle this
                    } else if (familiarityRadios.length > 0 && !firstErrorElement) {
                         firstErrorElement = familiarityRadios[0]; // Fallback to first radio
                    }
                }
            }


            // 2. Check Each Annotation Group (Required)
            annotationFieldsets.forEach(fieldset => {
                 const questionIndex = fieldset.getAttribute('data-question-index');
                 const annotationRadios = fieldset.querySelectorAll('input[type="radio"][name^="annotation_"]'); // More specific selector
                 const annotationSelected = Array.from(annotationRadios).some(radio => radio.checked);

                 if (!annotationSelected) {
                     console.warn(`Validation Fail: Annotation missing for question index ${questionIndex}`);
                     validationPassed = false;
                     if (!firstErrorElement) firstErrorElement = fieldset;
                     fieldset.classList.add('validation-error-highlight');
                     // fieldset.style.borderColor = 'red';
                 } else {
                     // 3. Check Offensiveness Rating *if* annotation is "Stereotype" (Conditionally Required)
                     const stereotypeRadio = fieldset.querySelector('input[value="Stereotype"]');
                     if (stereotypeRadio && stereotypeRadio.checked) {
                         const ratingContainer = document.getElementById(`rating_container_${questionIndex}`);
                         // Find the fieldset *inside* the container for highlighting
                         const ratingFieldset = ratingContainer ? ratingContainer.querySelector('fieldset') : null;
                         if (!ratingContainer || !ratingFieldset) {
                              console.error("Cannot find rating container or fieldset for validation."); return; // Skip validation for this item if elements missing
                         }
                         const ratingRadios = ratingContainer.querySelectorAll('input[type="radio"][name^="offensiveness_"]');
                         const ratingSelected = Array.from(ratingRadios).some(radio => radio.checked);

                         if (!ratingSelected) {
                             console.warn(`Validation Fail: Offensiveness rating missing for Stereotype at index ${questionIndex}`);
                             validationPassed = false;
                             if (!firstErrorElement) firstErrorElement = ratingFieldset; // Highlight the inner fieldset
                             ratingFieldset.classList.add('validation-error-highlight');
                             // ratingFieldset.style.borderColor = 'red';
                         }
                     }
                 }
            }); // End loop through annotation fieldsets


             // If validation failed overall, prevent submission and provide feedback
             if (!validationPassed) {
                 event.preventDefault(); // Stop the form submission
                 alert('Please complete all required fields (*).\nLook for the highlighted sections.');

                 // Scroll to and focus the first element identified with an issue
                 if (firstErrorElement) {
                     firstErrorElement.scrollIntoView({ behavior: 'smooth', block: 'center' });

                     // Add focus styling (browser default or custom via CSS :focus)
                     // Temporarily add stronger outline directly for emphasis
                     firstErrorElement.style.outline = '3px solid red';
                     // Attempt to focus the first radio/input within the fieldset if possible
                     const firstInput = firstErrorElement.querySelector('input[type="radio"], input[type="text"], select');
                     if(firstInput) {
                         firstInput.focus({ preventScroll: true }); // Focus without scrolling again
                     } else {
                        firstErrorElement.focus({ preventScroll: true }); // Focus the fieldset itself
                     }


                     // Remove the direct outline style after a short delay
                     setTimeout(() => {
                         if (firstErrorElement) firstErrorElement.style.outline = '';
                         // Note: .validation-error-highlight class remains until next submit attempt
                     }, 3500);
                 }
             }
             // If validationPassed is true, the form submits normally
        });
    } // End if(quizForm)

}); // End of DOMContentLoaded