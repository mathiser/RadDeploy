for fol in base storescp fingerprinter consumer storescu flow_tracker file_storage static_storage
do
  cd $fol
  bash build.sh
  cd ..
done