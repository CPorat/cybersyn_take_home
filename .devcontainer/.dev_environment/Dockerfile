
FROM continuumio/miniconda3:22.11.1

# update and clean
RUN apt-get update \
    && apt-get install -y \
    build-essential \
    libssl-dev \
    vim \
    git \
    gcc \
    g++ \
    make \
    cmake \
    curl \
    wget \
    jq \
    bat \
    bash-completion \
    ruby-full \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# env
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

# install navi
RUN BIN_DIR=/usr/bin curl -sL https://raw.githubusercontent.com/denisidoro/navi/master/scripts/install | bash
RUN git clone https://github.com/denisidoro/cheats.git /root/.local/share/navi/cheats/cheats

# install zsh
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.5/zsh-in-docker.sh)" -- \
    -p colored-man-pages \
    -p https://github.com/zsh-users/zsh-autosuggestions
COPY .devcontainer/.dev_environment/powerlevel/.p10k_HACK.zsh root/.p10k.zsh
RUN echo "[[ ! -f /root/.p10k.zsh ]] || source /root/.p10k.zsh" >> ~/.zshrc

ENV SHELL /usr/bin/zsh

# install colorls
RUN gem install colorls
RUN echo "source $(dirname $(gem which colorls))/tab_complete.sh" >> ~/.zshrc

# install diff-so-fancy
RUN git clone --depth=1 https://github.com/so-fancy/diff-so-fancy.git /root/diff-so-fancy
ENV PATH=$PATH:/root/diff-so-fancy

# install nerd fonts
RUN git clone --depth=1 https://github.com/powerline/fonts.git \
    && sh fonts/install.sh \
    && rm -rf fonts

# create and run conda env
RUN conda create --name cybersyn_takehome python=3.10 -y \
    && conda init zsh \
    && conda init bash

# install packages
COPY .devcontainer/.dev_environment/requirements/requirements.txt requirements.txt
RUN conda init bash && . ~/.bashrc \
    && conda activate cybersyn_takehome \
    && pip install --upgrade pip \
    && pip install --ignore-installed -r requirements.txt \
    && rm -rf /root/.cache \
    && rm -rf cybersyn_takehome/*

# install fzf-dbt
RUN git clone --depth 1 https://github.com/junegunn/fzf.git /root/.fzf
RUN /root/.fzf/install --key-bindings --completion --update-rc

ENV FZF_DBT_PATH=/root/.fzf-dbt/fzf-dbt.sh
ENV FZF_DBT_DIR=/root/.fzf-dbt
RUN mkdir -p /root/.fzf-dbt
RUN curl -L https://raw.githubusercontent.com/Infused-Insight/fzf-dbt/main/src/fzf_dbt.sh > /root/.fzf-dbt/fzf-dbt.sh
ENV FZF_DBT_PREVIEW_CMD="cat {}"
ENV FZF_DBT_HEIGHT=80%
ENV FZF_DBT_PREVIEW_CMD='batcat --theme OneHalfLight --color=always --style=numbers {}'
RUN echo "source /root/.fzf-dbt/fzf-dbt.sh" >> ~/.zshrc

# zsh aliases
RUN echo "alias zedit='vim ~/.zshrc'" >> ~/.zshrc
RUN echo "alias work='conda activate cybersyn_takehome && cd /cybersyn_takehome'" >> ~/.zshrc
RUN echo "alias newsh='exec $SHELL && source ~/.zshrc'" >> ~/.zshrc
# RUN echo "alias bat='batcat'" >> ~/.zshrc

# functions
RUN echo 'function dbtdd() { \
    dbt "$@" --vars "dev_disable: true" \
    }' >> ~/.zshrc

# workdir
WORKDIR /cybersyn_takehome

# dotenv copy
COPY .env .env

# set pythonpath
ENV PYTHONPATH="${PYTHONPATH}:/cybersyn_takehome"

# entry
ENTRYPOINT [ "/bin/zsh" ]
