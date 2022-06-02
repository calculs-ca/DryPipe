

def bash_shebang():
    return "#!/usr/bin/env bash"

BASH_TASK_FUNCS_AND_TRAPS = """


function __check_bash_version () {
    __bash_version_fail="DryPipe requires bash version >= 4.3.x, current version is: $BASH_VERSION"
    
    if [ -z "${BASH_VERSINFO}" ] || [ -z "${BASH_VERSINFO[0]}" ]; then
        echo $__bash_version_fail >&2         
        __transition_to_failed "" ""
    fi
                
    if [[ ${BASH_VERSINFO[0]} -lt 4  || ( ${BASH_VERSINFO[0]} -eq 4  && ${BASH_VERSINFO[1]} -lt 3 ) ]]; then 
        echo $__bash_version_fail >&2
        __transition_to_failed        
    fi
}    
                
function __read_task_state () {

    local count_for_sanity=$(find $__control_dir/state.* | wc -l)

    if [[ $count_for_sanity -ne 1 ]]; then
        echo "CORRUPTED_CONTROL_DIR: exactly one state file is required, found $count_for_sanity in $__control_dir" >&2
        exit 1
    fi

    local __state_file=$(ls $__control_dir/state.* | tr -d '[:space:]')
    local __state_file_name=$(basename $__state_file)
    export __state_file_abs_path="$__control_dir/$__state_file_name"
    local state_parts=(${__state_file_name//./ })

    export __state_name=${state_parts[1]}

    export __next_step_number=$(( ${state_parts[2]} + 0 ))
    
   if [[ $__state_name != "launched" && $__state_name != "scheduled" && $__state_name != "failed" && $__state_name != "timed-out" ]]; then

        if [[ $__arg_1 == "--force" ]]; then
            echo "force launching task $__task_key"
        else
            echo "INVALID_STATE_FOR_LAUNCHING: task state must be either launched or scheduled, state file: $__state_file" >&2
            echo "If you want to launch task script manualy, use --force as first argument" >&2
            exit 1
        fi
    fi
    
}

function __invoke_step () {    
    if [[ $__is_slurm == "True" ]]; then
        $__step_command &
        wait $!
    else 
        $__step_command
    fi    
}

function __touch_history () {
    touch $__control_dir/step_start
}

function __append_history () {

    local __t=$(date +"%FT%T.%5N" -d @$(stat -c %.5Y $__control_dir/step_start ))
    local __step_to_record=$__next_step_number
    
    if [[ $__state_name == "step-completed" ]]; then        
        __step_to_record=$((__next_step_number-1))
    elif [[ $__state_name == "completed-unsigned" ]]; then
        __step_to_record=""
    fi
    
    echo "$__state_name\t$__t\t$__step_to_record" >> "$__control_dir/history.tsv"                 
}

function __transition_to_next_state () {

    local next_state_file_abs_path="$__control_dir/state.$1.${__next_step_number}"

    mv $__state_file_abs_path $next_state_file_abs_path

    __state_name=$1
    __state_file_abs_path=$next_state_file_abs_path
            
    # touch $__pipeline_instance_dir/.drypipe/state.*  || true        
}

function __transition_to_failed () {
     
    __append_history
    __touch_history
    
    echo "task failed"
    local parent_lineno="$1"
    echo "Failed at line ${parent_lineno}"  >&2
    __transition_to_next_state "failed"
    exit 1
}

function __transition_to_timed_out () {

    __append_history
    __touch_history
        
    echo "task timed out"
    echo "task timed out"  >&2
    __transition_to_next_state "timed-out"    
    exit 1
}

function __transition_to_step_started () {

    __append_history
    __touch_history
    
    __transition_to_next_state "step-started"
}

function __transition_to_step_completed () {

    __append_history
    __touch_history
    
    __next_step_number=$((__next_step_number+1))
    __transition_to_next_state "step-completed"
}

function __transition_to_completed () {

    __append_history
    __touch_history    

    mv "$__state_file_abs_path" "$__control_dir/state.completed-unsigned"

    rm -f $__control_dir/out.sig
    
    __state_name="completed-unsigned"
    __touch_history
    __append_history    
}

function __container_image_path () {

    if [ -f "$1" ]; then
        echo "$1"    
    elif [ -f "$__containers_dir/$1" ]; then
        echo "$__containers_dir/$1"
    else 
        echo "container $1 not found" >&2
        echo "defined containers_dir=$__containers_dir" >&2
        exit 1;       
    fi        
}

function __add_binding_for_singularity_if_required () {

    if [[ ! $____singularity_bindings_done == "True" ]]; then
        
        local __root_dir_of_script=$(echo "$__pipeline_code_dir" | cut -d "/" -f2)
        local __fs_type=$(stat -f -L -c %T "/$__root_dir_of_script")
        if [[ $__fs_type == "autofs" || $__fs_type == "nfs" || $__fs_type == "zfs" ]]; then
    
            local __bind="/$__root_dir_of_script:/$__root_dir_of_script"
            
            if [ -z ${SINGULARITY_BIND+x} ]; then 
              export SINGULARITY_BIND=$__bind         
            else 
              export SINGULARITY_BIND=$__bind,$SINGULARITY_BIND
            fi            
        fi
    
        if [[ $__is_slurm == "True" ]]; then
            local __root_of_scratch=$(echo "$__scratch_dir" | cut -d "/" -f2)            
            export SINGULARITY_BIND=$SINGULARITY_BIND,/$__root_of_scratch:/$__root_of_scratch
        fi    
        export ____singularity_bindings_done=True
    fi    
}    

"""

__BASH_SIGN_FILES = """
function __sign_file () {
  fn=$(basename $1)
  if [ ! -f "$1" ]; then
     echo "warning: task $__task_key didn't produce output file $fn as specified"
  else
     fn="$__control_dir/$__sig_dir/$fn.sig"
%s
     sha1sum $1 > $fn
%s
  fi
}

function __sign_files () {

    for f in $__fileset_to_sign; do
        __sign_file $f
    done

    for f in $(echo $__file_list_to_sign | sed "s/,/ /g"); do
        __sign_file $f
    done
}

"""

BASH_SIGN_FILES = __BASH_SIGN_FILES % ("", "")

BASH_SIGN_FILES_IF_NEWER = __BASH_SIGN_FILES % (
    "    if [[ $1 -nt $fn ]]; then",
    "    fi"
)
