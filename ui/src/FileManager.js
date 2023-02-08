import React, { useMemo } from 'react'
import Uploady, { UPLOADER_EVENTS } from "@rpldy/uploady"
import { asUploadButton } from "@rpldy/upload-button";
import {FontAwesomeIcon} from "@fortawesome/react-fontawesome"
import {faChevronDown, faChevronRight, faUpload, faFolder, faFile} from '@fortawesome/free-solid-svg-icons'


const UploadButtonDiv = asUploadButton((props) => {
    return <span {...props} style={{ cursor: "pointer" }}>
        <button className={"button is-small"}>Upload Files</button>
    </span>
});

export const UploadButton = ({listeners, url, path}) =>
    <Uploady
        listeners={listeners}
        destination={{ url: `${url}${path}` }}
        grouped={false}
        debug={true}
        inputFieldName={"files"}
    >
        <UploadButtonDiv/>
    </Uploady>

export const FileManager = ({}) => {

   const listeners = useMemo(() => ({
        [UPLOADER_EVENTS.ITEM_FINISH]: (item) => {
            console.log(`Item Finish - ${item.id} : ${item.file.name}`)
        },
        [UPLOADER_EVENTS.ITEM_PROGRESS]: event => {
            console.log(`progress`, event)
        },
        [UPLOADER_EVENTS.BATCH_PROGRESS]: event => {
            console.log(`progress`, event)
        }
    }), [])

    const FolderView = () =>
        <div className="panel">
            <p className="panel-heading">Manage Files</p>

            <div className="panel-block">
                <span className="is-link is-outlined is-fullwidth">
                <div className="field is-grouped">
                    <p className="control">
                        <UploadButton
                            url={"/drypipe/upz"} path={"/zaz/zo"} listeners={listeners}
                        />
                    </p>
                    <p className="control">
                        <a className="button is-small">
                            Delete Selected
                        </a>
                    </p>
                    <p className="control">
                        /x/y/z
                    </p>
                </div>
                </span>
            </div>
            <a className="panel-block">
                ..
            </a>
            <a className="panel-block">
                <span className="panel-icon">
                    <FontAwesomeIcon icon={faFolder} />
                </span>
                bulma
            </a>
            <a className="panel-block">
                <span className="panel-icon">
                    <FontAwesomeIcon icon={faFile} />
                </span>
                bulma
            </a>
        </div>

    return <FolderView/>
}

