
export const taskFilterReducer = (state, action) => {

    switch (action.name) {
        case 'close':
            return null

        case 'open':
            return {
                keyPrefix: null,
                displayState: null
            }

        case 'selectTaskGroup':
            return {
                keyPrefix: action.keyPrefix,
                displayState: action.displayState
            }
        case 'selectOtherTaskGroup':
            return {
                ...state,
                keyPrefix: action.keyPrefix
            }
        case 'selectOtherDisplayState':
            return {
                ...state,
                displayState: action.displayState
            }
        default:
            throw Error(`unknown action ${action.name}`)
    }
}
