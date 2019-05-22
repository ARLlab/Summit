// https://www.w3schools.com/howto/howto_js_slideshow.asp

var slideIndex = 1;
showSlides(slideIndex);

// controls to move slides
function plusSlides(n) {
  showSlides(slideIndex += n);
}

// thumnail images controls
function currentSlide(n) {
  showSlides(slideIndex = n);
}

// main slideshow function
function showSlides(n) {
  var i;
  var slides = document.getElementsByClassName("mySlides");
  var dots = document.getElementsByClassName("dot");
  if (n > slides.length) {slideIndex = 1}
  if (n < 1) {slideIndex = slides.length}
  for (i = 0; i < slides.length; i++) {   // main slide movement for loop
      slides[i].style.display = "none";
  }
  for (i = 0; i < dots.length; i++) {     // this is the activated dots
      dots[i].className = dots[i].className.replace(" active", "");
  }
  // slideIndex++;
  // if (slideIndex > slides.length) {slideIndex = 1}  // this should auto move slides
  slides[slideIndex-1].style.display = "block";
  dots[slideIndex-1].className += " active";
  // setTimeout(showSlides, 4000);     // slides should change every 2 seconds
}
