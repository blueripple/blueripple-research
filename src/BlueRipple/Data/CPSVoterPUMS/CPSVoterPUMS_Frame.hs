{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE UndecidableInstances #-}

module BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Frame where
  ( module BlueRipple.Data.CPSVoterPUMS.CPSVoter_Path
  , module BlueRipple.Data.CPSVoterPUMS.CPSVoter_Frame
  )
where

import           BlueRipple.Data.CPSVoterPUMS.CPSVoter_Path

import qualified Frames.TH                     as F

F.tableTypes' cpsVoterPUMSRowGen


