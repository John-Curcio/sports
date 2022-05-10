import torch
from sklearn.utils import shuffle, gen_batches
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from tqdm import tqdm
        

class SymmetricModel(torch.nn.Module):
    
    def __init__(self, input_dim, width_1, width_2):
        super().__init__()
        self.linear1 = torch.nn.Linear(input_dim, width_1)
        self.activation = torch.nn.Tanh() # tanh(x) = -tanh(-x)
        self.linear2 = torch.nn.Linear(width_1, width_2)
        # take diff
        self.linear3 = torch.nn.Linear(width_2, 1, bias=False)
        self.softmax = torch.nn.Sigmoid()
        
    def forward(self, ml_logit, x_f, x_o):
        x_f = self.linear1(x_f)
        x_f = self.activation(x_f)
        x_f = self.linear2(x_f)
        x_f = self.activation(x_f)
        
        x_o = self.linear1(x_o)
        x_o = self.activation(x_o)
        x_o = self.linear2(x_o)
        x_o = self.activation(x_o)
        
        x_diffs = x_f - x_o
        x_diffs = self.linear3(x_diffs)
        y_hat = self.softmax(ml_logit + x_diffs)
        return y_hat
    
class SymmetricModelWrapper(object):
    
    def __init__(self, fighter_cols, opponent_cols, n_pca=16, 
                 width_1=16, width_2=16, lr=0.001, n_epochs=30, batch_size=64):
        self.fighter_cols = fighter_cols
        self.opponent_cols = opponent_cols
        assert len(fighter_cols) == len(opponent_cols) # fuck style
        self.n_pca = n_pca
        self.width_1 = width_1
        self.width_2 = width_2
        self.lr = lr
        self.n_epochs = n_epochs
        self.batch_size = batch_size
        self._model = None
        
    def _get_X(self, train_df, test_df):
        X_temp = np.concatenate([train_df[self.fighter_cols], 
                                 train_df[self.opponent_cols]])
        scale_ = np.sqrt((X_temp**2).mean(0))
        pca = PCA(n_components=self.n_pca, whiten=True)
        pca.fit(X_temp / scale_)
        
        X_f_train = pca.transform(train_df[self.fighter_cols].values / scale_)
        X_o_train = pca.transform(train_df[self.opponent_cols].values / scale_)
        
        X_f_test = pca.transform(test_df[self.fighter_cols].values / scale_)
        X_o_test = pca.transform(test_df[self.opponent_cols].values / scale_)
        
        return X_f_train, X_o_train, X_f_test, X_o_test
    
    def _train(self, X_f_train, X_o_train, y_train, ml_logit_train):
        input_dim = X_f_train.shape[1]
        self._model = SymmetricModel(input_dim, self.width_1, self.width_2)
        optimizer = torch.optim.Adam(self._model.parameters(), lr=self.lr)
        loss_fn = torch.nn.BCELoss() # binary cross entropy
        for epoch in tqdm(range(self.n_epochs)):
            # shuffle dataset
            shuffle_inds = shuffle(range(X_f_train.shape[0]))
            X_f = torch.Tensor(X_f_train[shuffle_inds,:])
            X_o = torch.Tensor(X_o_train[shuffle_inds,:])
            ml = torch.Tensor(ml_logit_train[shuffle_inds]).view(-1,1)
            y = torch.Tensor(y_train[shuffle_inds]).view(-1,1)
            # iterate over batches
            for batch_inds in gen_batches(X_f.shape[0], self.batch_size):
                optimizer.zero_grad()
                y_hat = self._model(ml[batch_inds], X_f[batch_inds,:], X_o[batch_inds,:])
                loss = loss_fn(y_hat, y[batch_inds])
                loss.backward()
                optimizer.step()
        
    def fit_predict(self, train_df, test_df):
        X_f_train, X_o_train, X_f_test, X_o_test = self._get_X(train_df, test_df)
        
        y_train = train_df["targetWin"].values
        y_test = test_df["targetWin"].values

        ml_logit_train = logit(train_df["p_fighter_implied"]).values
        ml_logit_test = logit(test_df["p_fighter_implied"]).values
        
        self._train(X_f_train, X_o_train, y_train, ml_logit_train)
        
        y_hat = self._model(
            torch.Tensor(ml_logit_test).view(-1,1), 
            torch.Tensor(X_f_test),
            torch.Tensor(X_o_test),
        )
        return y_hat.detach().numpy()[:,0]
            